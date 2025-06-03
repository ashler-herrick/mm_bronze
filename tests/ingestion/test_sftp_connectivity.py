import socket

def test_port_only(host="localhost", port=2222):
    """Test if port is listening without reading data"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception as e:
        print(f"Error testing port: {e}")
        return False

if __name__ == "__main__":
    print("Testing SFTP server port availability...")
    
    if test_port_only():
        print("Port 2222 is listening and accepting connections")
        print("ℹUse the full SFTP test to verify complete functionality")
    else:
        print("Port 2222 is not accessible")
        exit(1)