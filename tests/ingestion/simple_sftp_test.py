#!/usr/bin/env python3
"""
Simple test script to upload a file to our custom SFTP server.
"""

import paramiko
import sys
import os
import time
import tempfile

# Configuration
HOST = "localhost"
PORT = 2222
USERNAME = "alice"
PASSWORD = "secret"
REMOTE_DIR = "/uploads"

def create_test_file():
    """Create a small test file."""
    content = f"""Test file created at {time.strftime('%Y-%m-%d %H:%M:%S')}
    This is a test upload to verify the SFTP server is working.
    File size: {len('Test content')} bytes
    Random data: {os.urandom(16).hex()}
    """
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt', prefix='sftp_test_') as f:
        f.write(content)
        test_file = f.name
    
    print(f"Created test file: {test_file}")
    print(f"File size: {os.path.getsize(test_file)} bytes")
    return test_file

def upload_file(local_file, remote_file):
    """Upload a file via SFTP."""
    print(f"\n--- SFTP Upload Test ---")
    print(f"Local file: {local_file}")
    print(f"Remote file: {remote_file}")
    print(f"Server: {HOST}:{PORT}")
    print(f"User: {USERNAME}")
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print("\n1. Connecting to SSH server...")
        ssh.connect(
            hostname=HOST,
            port=PORT,
            username=USERNAME,
            password=PASSWORD,
            look_for_keys=False,
            allow_agent=False,
            timeout=10,
        )
        print("✓ SSH connection successful")
        
        print("\n2. Opening SFTP session...")
        # Set a shorter timeout for SFTP operations
        transport = ssh.get_transport()
        transport.set_keepalive(30)
        
        sftp = ssh.open_sftp()
        sftp.get_channel().settimeout(30)  # 30 second timeout
        print("✓ SFTP session opened")
        
        print("\n3. Ensuring remote directory exists...")
        try:
            # Try to list the directory first
            sftp.listdir(REMOTE_DIR)
            print(f"✓ Remote directory {REMOTE_DIR} exists")
        except FileNotFoundError:
            print(f"⚠ Remote directory {REMOTE_DIR} doesn't exist, creating it...")
            try:
                sftp.mkdir(REMOTE_DIR)
                print(f"✓ Created remote directory {REMOTE_DIR}")
            except Exception as e:
                print(f"⚠ Could not create directory: {e}")
        
        print("\n4. Uploading file...")
        start_time = time.time()
        sftp.put(local_file, remote_file)
        upload_time = time.time() - start_time
        print(f"✓ Upload completed in {upload_time:.2f} seconds")
        
        print("\n5. Verifying upload...")
        try:
            remote_stat = sftp.stat(remote_file)
            local_size = os.path.getsize(local_file)
            remote_size = remote_stat.st_size
            
            print(f"Local file size:  {local_size:,} bytes")
            print(f"Remote file size: {remote_size:,} bytes")
            
            if remote_size == local_size:
                print("✓ File sizes match - upload verified!")
                success = True
            else:
                print("✗ File size mismatch!")
                success = False
                
        except Exception as e:
            print(f"⚠ Could not verify remote file: {e}")
            success = False
        
        print("\n6. Listing remote directory...")
        try:
            files = sftp.listdir(REMOTE_DIR)
            print(f"Files in {REMOTE_DIR}: {files}")
        except Exception as e:
            print(f"⚠ Could not list remote directory: {e}")
        
        sftp.close()
        ssh.close()
        
        return success
        
    except Exception as e:
        print(f"✗ Upload failed: {e}")
        import traceback
        traceback.print_exc()
        if ssh:
            try:
                ssh.close()
            except:
                pass
        return False

def main():
    """Main function."""
    print("=" * 50)
    print("SFTP Server Upload Test")
    print("=" * 50)
    
    # Create test file
    test_file = create_test_file()
    remote_file = f"{REMOTE_DIR}/{os.path.basename(test_file)}"
    
    try:
        # Upload file
        success = upload_file(test_file, remote_file)
        
        if success:
            print("\n🎉 Upload test PASSED!")
            print("✓ File uploaded successfully")
            print("✓ Server handled the connection properly")
            print("✓ Check server logs for Kafka event publication")
        else:
            print("\n❌ Upload test FAILED!")
            sys.exit(1)
            
    finally:
        # Clean up test file
        try:
            os.unlink(test_file)
            print(f"\nCleaned up test file: {test_file}")
        except:
            pass

if __name__ == "__main__":
    main()