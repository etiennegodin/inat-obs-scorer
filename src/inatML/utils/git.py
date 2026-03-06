import subprocess


def get_git_hash(short=False):
    """
    Retrieves the current Git commit hash using subprocess.

    Args:
        short (bool): If True, returns the shortened 7-character hash.

    Returns:
        str: The Git commit hash.
    """
    command = (
        ["git", "rev-parse", "--short", "HEAD"]
        if short
        else ["git", "rev-parse", "HEAD"]
    )
    try:
        # Use check_output for simple command execution and error checking
        result = subprocess.check_output(command, stderr=subprocess.PIPE, text=True)
        return result.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error getting git hash: {e.stderr}")
        return "UNKNOWN"
    except FileNotFoundError:
        print("Git executable not found. Make sure Git is installed and in your PATH.")
        return "UNKNOWN"
