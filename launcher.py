import os
import sys
import subprocess
import time
import webbrowser
import threading

def open_browser():
    # Wait 2 seconds for the Django server to boot up, then open the browser
    time.sleep(2)
    webbrowser.open("http://127.0.0.1:49854/dashboard/")

def main():
    # Windows API Flags
    CREATE_NO_WINDOW = 0x08000000   # Runs completely silently
    CREATE_NEW_CONSOLE = 0x00000010 # Pops open a new visible window

    # Get the actual installation folder
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        
    venv_dir = os.path.join(base_dir, "venv")
    
    # Paths to the virtual environment executables
    python_exe = os.path.join(venv_dir, "Scripts", "python.exe")
    pip_exe = os.path.join(venv_dir, "Scripts", "pip.exe")
    manage_py = os.path.join(base_dir, "manage.py")

    # =================================================================
    # 1. FIRST TIME LAUNCH: Pop open a temporary visible console
    # =================================================================
    if not os.path.exists(python_exe):
        # We chain CMD commands together to create a nice setup UI
        setup_command = (
            "title AuditCore PRO - Initial Setup & "
            "color 0B & "
            "echo =============================================================== & "
            "echo                AuditCore PRO - System Engine                    & "
            "echo =============================================================== & "
            "echo. & "
            "echo [SETUP] First-time launch detected. Building local environment... & "
            "python -m venv venv & "
            "echo [SETUP] Installing required system libraries... & "
            "venv\\Scripts\\pip.exe install -r requirements.txt & "
            "echo. & "
            "echo [SETUP] Environment configured successfully! Starting System... & "
            "timeout /t 3 >nul"
        )
        
        # This spawns the visible window just for the installation phase
        subprocess.run(["cmd.exe", "/c", setup_command], cwd=base_dir, creationflags=CREATE_NEW_CONSOLE)


    # =================================================================
    # 2. EVERY LAUNCH: Start Web Browser
    # =================================================================
    threading.Thread(target=open_browser, daemon=True).start()


    # =================================================================
    # 3. EVERY LAUNCH: Start Django Silently (No Window)
    # =================================================================
    try:
        subprocess.run(
            [python_exe, manage_py, "runserver", "49854", "--noreload"], 
            cwd=base_dir, 
            creationflags=CREATE_NO_WINDOW, # Forces the server to hide
            stdout=subprocess.DEVNULL, 
            stderr=subprocess.DEVNULL
        )
    except Exception:
        pass

if __name__ == "__main__":
    main()