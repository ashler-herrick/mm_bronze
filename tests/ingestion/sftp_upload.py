import paramiko
import sys
import os

# ─── Configuration ─────────────────────────────────────────────────────────────

HOST = "localhost"  # your SFTP server host (or container DNS name)
PORT = 2222  # SFTP port as mapped by Docker
USERNAME = "alice"  # SFTP user
PASSWORD = "secret"  # or set to None if you use a key

LOCAL_FILE = "/home/ashler/Documents/mm_bronze/tests/test_data/dicom/Abe604_Frami345_b8dd1798-beef-094d-1be4-f90ee0e6b7d51.2.840.99999999.26401232.758647660200.dcm"
REMOTE_DIR = "/uploads"
REMOTE_FILE = os.path.basename(LOCAL_FILE)

# ─── End configuration ─────────────────────────────────────────────────────────


def main():
    # 1) Verify local file
    if not os.path.isfile(LOCAL_FILE):
        print(f"[ERROR] Local file not found: {LOCAL_FILE}", file=sys.stderr)
        sys.exit(1)
    print(f"[OK] Local file exists: {LOCAL_FILE}")

    # 2) Connect SSH (password-only)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(
            hostname=HOST,
            port=PORT,
            username=USERNAME,
            password=PASSWORD,
            look_for_keys=False,
            allow_agent=False,
            timeout=10,
        )
        print(f"[OK] SSH connected to {HOST}:{PORT} as {USERNAME}")
    except Exception as e:
        print(f"[ERROR] SSH connection failed: {e}", file=sys.stderr)
        sys.exit(1)

    # 3) Open SFTP
    try:
        sftp = ssh.open_sftp()
        print("[OK] SFTP session opened")
    except Exception as e:
        print(f"[ERROR] Opening SFTP failed: {e}", file=sys.stderr)
        ssh.close()
        sys.exit(1)

    # 4) Upload directly to /uploads/<filename>
    remote_path = os.path.join(REMOTE_DIR, REMOTE_FILE)
    # perform the upload
    print(f"[INFO] Uploading → {LOCAL_FILE} to {remote_path}")
    try:
        sftp.put(LOCAL_FILE, remote_path)
        print(f"[OK] Upload of {REMOTE_FILE} to {remote_path} successful")
    except Exception as e:
        print(f"[ERROR] Upload failed: {e}", file=sys.stderr)
        sftp.close()
        ssh.close()
        sys.exit(1)


if __name__ == "__main__":
    main()
