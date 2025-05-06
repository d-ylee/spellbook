import subprocess
import time

def connect_with_netcat(host, port, trustfile):
    """
    Attempts to connect to a server using netcat with SSL verification.

    Args:
        host (str): The hostname or IP address of the server.
        port (int): The port number to connect to.
        trustfile (str): The path to the SSL CA certificate (don't forget intermediates and root)

    Returns:
        bool: True if the connection was successful, False otherwise.
    """
    command = [
        "nc",
        "-v",
        "-z",
        "--ssl-verify",
        f"--ssl-trustfile={trustfile}",
        host,
        str(port)
    ]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate(timeout=60)  # Give it a reasonable timeout

    if process.returncode == 0:
        print(f"Successfully connected to {host}:{port}", flush=True)
        return True
    else:
        print(f"Failed to connect to {host}:{port}", flush=True)
        print(f"Error: {stderr.decode()}", flush=True)
        return False

if __name__ == "__main__":
    target_host = "usdf-fts3.slac.stanford.edu"
    target_port = 8449
    ssl_trust_file = "InCommon-RSA-IGTF-Server-CA-3.pem"  # Make sure this file exists in the same directory

    successful_connections = 0
    failed_connections = 0
    num_attempts = 500  # You can adjust the number of connection attempts

    print(f"Attempting to connect to {target_host}:{target_port} {num_attempts} times...", flush=True)

    for i in range(num_attempts):
        print(f"\nAttempt {i + 1}:", flush=True)
        if connect_with_netcat(target_host, target_port, ssl_trust_file):
            successful_connections += 1
        else:
            failed_connections += 1
        time.sleep(1)  # Add a small delay between attempts if needed

    print("\n--- Connection Summary ---")
    print(f"Successful Connections: {successful_connections}")
    print(f"Failed Connections: {failed_connections}")
