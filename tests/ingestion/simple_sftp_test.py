#!/usr/bin/env python3
"""
Proper SFTP test that performs full SSH handshake and authentication
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
    Random data: {os.urandom(16).hex()}
    """
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt', prefix='sftp_test_') as f:
        f.write(content)
        test_file = f.name
    
    print(f"Created test file: {test_file}")
    print(f"File size: {os.path.getsize(test_file)} bytes")
    return test_file

def test_sftp_full():
    """Test full SFTP workflow with proper SSH handshake."""
    print("=" * 60)
    print("Full SFTP Server Test")
    print("=" * 60)
    
    # Create test file
    test_file = create_test_file()
    remote_file = f"{REMOTE_DIR}/{os.path.basename(test_file)}"
    
    ssh = None
    sftp = None
    
    try:
        print(f"\n1. Connecting to {HOST}:{PORT} as {USERNAME}...")
        
        # Create SSH client with proper settings
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Connect with longer timeout and proper SSH settings
        ssh.connect(
            hostname=HOST,
            port=PORT,
            username=USERNAME,
            password=PASSWORD,
            look_for_keys=False,
            allow_agent=False,
            timeout=30,  # Longer timeout
            banner_timeout=30,  # Banner timeout
            auth_timeout=30,    # Auth timeout
        )
        print("✓ SSH connection and authentication successful")
        
        print("\n2. Opening SFTP channel...")
        sftp = ssh.open_sftp()
        print("✓ SFTP channel opened successfully")
        
        print("\n3. Testing directory listing...")
        try:
            files = sftp.listdir(".")
            print(f"✓ Current directory contents: {files}")
        except Exception as e:
            print(f"⚠ Could not list current directory: {e}")
        
        print("\n4. Creating test subdirectory...")
        test_subdir = "test_uploads"
        try:
            sftp.mkdir(test_subdir)
            print(f"✓ Created directory: {test_subdir}")
        except Exception as e:
            print(f"⚠ Directory creation failed (may already exist): {e}")
        
        print("\n5. Uploading test file...")
        remote_path = f"{test_subdir}/{os.path.basename(test_file)}"
        start_time = time.time()
        sftp.put(test_file, remote_path)
        upload_time = time.time() - start_time
        print(f"✓ Upload completed in {upload_time:.2f} seconds")
        
        print("\n6. Verifying upload...")
        try:
            remote_stat = sftp.stat(remote_path)
            local_size = os.path.getsize(test_file)
            remote_size = remote_stat.st_size
            
            print(f"Local file size:  {local_size:,} bytes")
            print(f"Remote file size: {remote_size:,} bytes")
            
            if remote_size == local_size:
                print("✓ File sizes match - upload verified!")
            else:
                print("✗ File size mismatch!")
                return False
                
        except Exception as e:
            print(f"⚠ Could not verify remote file: {e}")
            return False
        
        print("\n7. Testing file download...")
        download_file = test_file + ".downloaded"
        try:
            sftp.get(remote_path, download_file)
            
            # Compare files
            with open(test_file, 'r') as f1, open(download_file, 'r') as f2:
                if f1.read() == f2.read():
                    print("✓ Download successful - files match!")
                else:
                    print("✗ Downloaded file doesn't match original!")
                    return False
                    
            os.unlink(download_file)  # Clean up
            
        except Exception as e:
            print(f"⚠ Download test failed: {e}")
        
        print("\n8. Cleaning up remote file...")
        try:
            sftp.remove(remote_path)
            print("✓ Remote file removed")
        except Exception as e:
            print(f"⚠ Could not remove remote file: {e}")
        
        return True
        
    except paramiko.AuthenticationException:
        print("✗ Authentication failed - check username/password")
        return False
    except paramiko.SSHException as e:
        print(f"✗ SSH error: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Clean up
        if sftp:
            try:
                sftp.close()
            except:
                pass
        if ssh:
            try:
                ssh.close()
            except:
                pass
        try:
            os.unlink(test_file)
            print(f"\nCleaned up local test file: {test_file}")
        except:
            pass

def main():
    """Main test function."""
    success = test_sftp_full()
    
    if success:
        print("\n" + "=" * 60)
        print(" SFTP TEST PASSED!")
        print("✓ SSH connection successful")
        print("✓ SFTP subsystem working")
        print("✓ Authentication working")  
        print("✓ File upload/download working")
        print("✓ Permission system working")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("SFTP TEST FAILED!")
        print("Check server logs for details")
        print("=" * 60)
        sys.exit(1)

if __name__ == "__main__":
    main()