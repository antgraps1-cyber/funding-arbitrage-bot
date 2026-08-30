import subprocess, os, sys

# Start the bots as background processes
subprocess.Popen([sys.executable, "-u", "funding_bot.py"], stdout=open("funding.log", "w"), stderr=subprocess.STDOUT)
subprocess.Popen([sys.executable, "-u", "price_bot.py"], stdout=open("price.log", "w"), stderr=subprocess.STDOUT)

# Start gunicorn as the foreground process (required by Railway)
port = os.environ.get("PORT", "5000")
os.execvp("gunicorn", ["gunicorn", "dashboard:app", "-b", f"0.0.0.0:{port}"])
