import os
import sys
import subprocess
import time

def run_command(command, desc):
    print(f"🚀 Running {desc}...")
    start = time.time()
    try:
        # Run and stream output
        process = subprocess.run(command, shell=True, check=False)
        duration = time.time() - start
        if process.returncode == 0:
            print(f"✅ {desc} passed in {duration:.2f}s")
            return True
        else:
            print(f"❌ {desc} failed in {duration:.2f}s")
            return False
    except Exception as e:
        print(f"💥 Error running {desc}: {e}")
        return False

def main():
    print("="*60)
    print("      HVPDB CI/CD Pipeline (Local Verification)      ")
    print("="*60)
    
    # 1. Environment Check
    print("\n[1/3] Environment Check")
    py_version = sys.version.split()[0]
    print(f"Python Version: {py_version}")
    
    # 2. Run Tests
    print("\n[2/3] Running Test Suite")
    test_cmd = f"{sys.executable} -m pytest tests -v"
    tests_passed = run_command(test_cmd, "Pytest Suite")
    
    # 3. Build Check (Optional but good for CI)
    print("\n[3/3] Build Verification")
    build_cmd = f"{sys.executable} setup.py check"
    build_passed = run_command(build_cmd, "Setup Check")
    
    print("\n" + "="*60)
    if tests_passed and build_passed:
        print("🎉 ALL CHECKS PASSED! Ready for deployment.")
        sys.exit(0)
    else:
        print("⛔ CI FAILED. Please fix errors above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
