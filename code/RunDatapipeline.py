import schedule
import subprocess
import sys
import time


def run_scripts():
    print("Start SDM")
    subprocess.run([sys.executable, "SourceDataModel.py"])

    print("Start DWH")
    subprocess.run([sys.executable, "DataWareHouse.py"])

    print("Beide scripts klaar")


# Elke 1 uur uitvoeren
schedule.every(1).hours.do(run_scripts)

print("Scheduler gestart...")

# Optioneel: meteen 1 keer uitvoeren bij start
run_scripts()

# Blijf draaien
while True:
    schedule.run_pending()
    time.sleep(1)