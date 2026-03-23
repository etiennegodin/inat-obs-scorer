import psutil

logical_cores = psutil.cpu_count(logical=True)
physical_cores = psutil.cpu_count(logical=False)

print(f"Logical Threads: {logical_cores}")
print(f"Physical Cores: {physical_cores}")

# Recommendation:
# For LightGBM, using physical cores often prevents 'thread thrashing'
best_n_jobs = physical_cores - 1
print(f"Recommended n_jobs to keep one core free: {best_n_jobs}")
