import asyncio, aiohttp
from aiolimiter import AsyncLimiter
from asyncio import Queue
from pprint import pprint
from typing import Union
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from ....db import DataBaseLoader

class inatApiRequest():
    def __init__(self, name:str,
                 endpoint:str,
                 key:str,
                 limit:int = None,
                explicit_params:dict = None,
                fields:dict = None,
                api_version:int = 2,
                per_page:int = 10):
        
        #Init api
        self.name = name
        self.url = f"https://api.inaturalist.org/v{api_version}/{endpoint}/{key}"

        if fields is not None:
            self.fields = f"({fields_to_string(fields)})"
        else:
            self.fields = None
        self.per_page = per_page
        self.explicit_params = explicit_params
        self.limit = limit
        super().__init__(name)
        
    async def _execute(self, context):
        
          # Set params 
        if self.explicit_params is not None:
            params = self.explicit_params
        else:
            params = {}
          
        #Set fields 
        if self.fields is not None:
            params['fields'] = self.fields
            
        params['per_page'] = self.per_page
        
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url, params=params, timeout=10) as r:
                                r.url
                                r.raise_for_status()
                                data = await r.json()    
                                if data and "results" in data:
                                    if self.limit is None:
                                        return data['results']
                                    elif self.limit == 1:
                                        return data['results'][0]
                                    else:
                                        return data['results'][:self.limit]

                                    # Put all results from this multi-ID request into queue
            

class inatApiClient(ClassStep):
    def __init__(self, name:str,
                 endpoint:str,
                 items_source:str,
                 items_key:str,
                 items_limit:int= None,
                params_key:str = None,
                explicit_params:dict = None,
                fields:dict = None,
                api_version:int = 2,
                limiter:int = 60,
                per_page:int = 10,
                limit:int = None,
                overwrite_table:bool = False,
                chunk_size:int = 1):
        
        #Init api
        self.name = name
        self.params_key = params_key
        self.base_url = f"https://api.inaturalist.org/v{api_version}/{endpoint}"

        self.items_source = items_source
        self.items_key = items_key
        self.items_limit = items_limit
        if fields is not None:
            self.fields = f"({fields_to_string(fields)})"
        else:
            self.fields = None
            
        self.explicit_params = explicit_params
        #Init fetch behaviour 
        self.overwrite = overwrite_table
        self.per_page = per_page
        self.limit = limit
        self.limiter = AsyncLimiter(limiter, 60)
        self.queue  = Queue()
        self.chunk_size = chunk_size
        #Init writer
        self.table_name = f"raw.{name}"

        super().__init__(name)


        
    async def _execute(self, context:PipelineContext):
        last_id = None
        con = context.con
        items_query = f"""SELECT DISTINCT {self.items_key},
                            FROM {self.items_source}
                            ORDER BY {self.items_key} ASC 
                            {f"LIMIT {self.items_limit}" if self.items_limit is not None else ""}
                            """
        items = context.con.execute(items_query).df()[self.items_key].to_list()
        self.logger.debug(f"{self.name} items: \n{items}")

        self.logger.debug(len(items))
        #Convert to int
        try:
            items = [int(item) for item in items]
        except Exception as e:
            self.logger.error(e)

        # Create table for data
        self._create_table(context)


        # Get last_id from table
        try:
            max_id = con.execute(f"SELECT MAX(item_key) FROM {self.table_name}").fetchone()[0] 
            min_id = con.execute(f"SELECT MIN(item_key) FROM {self.table_name}").fetchone()[0] 

            if max_id is not None and min_id is not None:
                max_id = int(max_id)
                min_id = int(min_id)
                if max_id > min_id:
                    last_id = max_id
                else:
                    last_id = min_id
        except Exception as e:
            self.logger.error(e)
        
            
        if self.overwrite and self.table_name in get_all_tables(con):    
            if last_id is not None:
                if await _ask_yes_no(f'Found existing table data on disk for {self.table_name}, do you want to overwrite all ? (y/n)'):
                    self._create_table(context) # -- forced
                    last_id = None
        
        # Filter items based on last processed ID (idempotent resume)
        if last_id is not None:
            self.logger.info(f"Resuming from last processed ID: {last_id}")
             
            # Filter items: keep only those > last_id (since ordered ASC)
            items = [item for item in items if item >= last_id]

            if not items:
                self.logger.info(f"All items already processed")
                return self.table_name
            
        self.logger.debug(len(items))

        
        # Apply limit if set
        if self.limit is not None:
            items = items[: self.limit]
        
        self.item_count = len(items)
        self.logger.info(f"Processing {self.item_count} items, starting from: {items[0] if items else 'N/A'}")
        
        # Chunk items for batch processing
        items_chunks = [items[i:i + self.chunk_size] for i in range(0, len(items), self.chunk_size)]
        self.logger.info(f"Processing {self.item_count} items in {len(items_chunks)} chunks of {self.chunk_size}")
        
        #Store count of observers (for self.logger)
        async with aiohttp.ClientSession() as session:
            writer_task = asyncio.create_task(self._write_data(con))
            
            # Create fetchers with batched IDs
            fetchers = []
            for chunk_idx, chunk in enumerate(items_chunks):
                # Batch IDs into groups for API requests (e.g., 10 IDs per request)
                ids_per_request = self.per_page
                for batch_start in range(0, len(chunk), ids_per_request):
                    batch_end = min(batch_start + ids_per_request, len(chunk))
                    batch_keys = chunk[batch_start:batch_end]
                    # Create comma-separated ID string
                    item_key = ','.join(str(key) for key in batch_keys)
                    fetchers.append(asyncio.create_task(
                        self._fetch_data(session, item_key, batch_start, chunk_idx)
                    ))
            
            await asyncio.gather(*fetchers)
            await self.queue.join()
            await self.queue.put(None)
            await writer_task
            
        return self.table_name

    def _create_table(self, context:PipelineContext):
        context.con.execute(f"CREATE TABLE IF NOT EXISTS  {self.table_name} (batch_idx INT, chunk_idx INT, item_key INT, json JSON)")

    async def _write_data(self, con):
        self.logger.info('Init writer task')
        loop = asyncio.get_event_loop()
        executor = ThreadPoolExecutor(max_workers=1)
        processed_count = 0
        batch_size = 50  # Insert multiple rows in a batch

        while True:
            item = await self.queue.get()
            if item is None:
                self.logger.info(f'Writer done, processed {processed_count} items')
                self.queue.task_done()
                break
            
            # Collect a batch of items
            batch = [item]
            while not self.queue.empty() and len(batch) < batch_size:
                try:
                    batch.append(self.queue.get_nowait())
                except:
                    break
            
            try:
                # Run blocking database batch insert in thread pool
                def batch_insert():
                    for batch_idx, chunk_idx, item_key, data in batch:
                        self.logger.debug(f'Saved item {item_key}')
                        con.execute(
                            f"INSERT INTO {self.table_name} VALUES (?, ?, ?, ?)",
                            (batch_idx, chunk_idx, item_key, json.dumps(data))
                        )
                    con.commit()  # Commit batch
                
                await loop.run_in_executor(executor, batch_insert)
                processed_count += len(batch)
                self.logger.debug(f'Inserted batch of {len(batch)} items')

            except Exception as e:
                self.logger.error(f"FAILED to insert batch: {e}")
            
            # Mark all items in batch as done
            for _ in batch:
                self.queue.task_done()
        
        executor.shutdown(wait=True)
        
    async def _fetch_data(self, session, item_key:str, batch_idx:int, chunk_idx:int=0):
        """Fetch data for multiple IDs in a single request using comma-separated ID string"""
        
        # Set params 
        if self.explicit_params is not None:
            params = self.explicit_params
        else:
            params = {}
            
        if self.params_key is not None:
            params[self.params_key] = item_key
            url = self.base_url
        else:
            url = self.base_url + (item_key)
          
        #Set fields 
        if self.fields is not None:
            params['fields'] = self.fields

        params['per_page'] = self.per_page
        
        async with self.limiter:
            try:
                async with session.get(url, params=params, timeout=10) as r:
                    self.logger.debug(r.url)
                    r.raise_for_status()
                    data = await r.json()    
                    if data and "results" in data:
                        results = data["results"]
                        
                        async def _put_in_queue(result_to_add):
                            try:
                                await self.queue.put((batch_idx, chunk_idx, item_key, result_to_add))
                                self.logger.debug(f"Fetched result for IDs {item_key}: {result_to_add.get('id', 'N/A')}")
                            except Exception as e:
                                self.logger.error(f"FAILED to queue result: {e}")
                        
                        # Put all results from this multi-ID request into queue
                        if isinstance(results,dict):
                            await _put_in_queue(results) # store dict directly

                        elif isinstance(results, list):
                            for result in data["results"]: # iterate list of dicts 
                                await _put_in_queue(result)
                                

                        self.logger.info(f"Fetched {len(data['results'])} results for IDs: {item_key} | {chunk_idx}/{self.item_count}")
                    else:
                        self.logger.warning(f"No results found for IDs {item_key}")
            except Exception as e:
                self.logger.error(r.url)
                self.logger.error(f"API request FAILED for IDs {item_key}: {e}")



# Convert to the special syntax
def fields_to_string(fields_dict, level=0):
    parts = []
    for key, value in fields_dict.items():
        if isinstance(value, dict):
            nested = fields_to_string(value, level + 1)
            parts.append(f"{key}:({nested})")
        elif value is True:
            parts.append(f"{key}:!t")
    return ','.join(parts)

