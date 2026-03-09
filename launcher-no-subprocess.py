import os
import sys
import subprocess
import time
import webbrowser
import threading

def open_browser():
    # Wait 2 seconds for the Django server to boot up, then open the browser
    time.sleep(2)
    webbrowser.open("http://127.0.0.1:3030/dashboard/")

def main():
    print("===============================================================")
    print("               AuditCore PRO - System Engine")
    print("===============================================================")
    print("\n[SYSTEM] Initializing...")

    # CRITICAL FIX 1: Get the actual installation folder (Program Files), 
    # not the temporary PyInstaller extraction folder.
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        
    venv_dir = os.path.join(base_dir, "venv")
    
    # Paths to the virtual environment executables
    python_exe = os.path.join(venv_dir, "Scripts", "python.exe")
    pip_exe = os.path.join(venv_dir, "Scripts", "pip.exe")
    manage_py = os.path.join(base_dir, "manage.py")
    req_file = os.path.join(base_dir, "requirements.txt")

    # 1. Check if Virtual Environment exists; if not, build it.
    if not os.path.exists(python_exe):
        print("[SETUP] First-time launch detected. Building local environment...")
        
        # CRITICAL FIX 2: Use the literal string "python" instead of sys.executable
        # to prevent the infinite loop. We also specify cwd=base_dir to ensure 
        # the venv is built in the exact right folder.
        subprocess.run(["python", "-m", "venv", "venv"], cwd=base_dir)
        
        if os.path.exists(req_file):
            print("[SETUP] Installing required system libraries...")
            subprocess.run([pip_exe, "install", "-r", "requirements.txt"], cwd=base_dir)
        
        print("[SETUP] Environment configured successfully!\n")

    # 2. Launch the Web Browser in the background
    print("[SYSTEM] Starting Database and Web Engine...")
    print("[SYSTEM] Please do not close this black window while using the application.\n")
    threading.Thread(target=open_browser, daemon=True).start()

    # 3. Start the Django Server
    try:
        subprocess.run([python_exe, manage_py, "runserver", "3030"], cwd=base_dir)
    except KeyboardInterrupt:
        print("\n[SYSTEM] Shutting down AuditCore PRO...")
    except Exception as e:
        print(f"\n[ERROR] System encountered an error: {e}")
        input("Press Enter to exit...")

if __name__ == "__main__":
    main()