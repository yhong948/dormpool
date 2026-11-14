"""
worker/main.py  —  Worker Agent entry point
Person B: run with:
  cd f:/dormpool/dormpool
  MASTER_URL=http://<master-ip>:8000 DORMPOOL_TOKEN=yourtoken python worker/main.py
"""
from worker.agent.w1_runtime import main
if __name__ == "__main__":
    main()
