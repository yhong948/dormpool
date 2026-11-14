"""
worker/agent/w6_control.py  —  W6 Local Control Interface
==========================================================
Person B owns this file.

Responsibility:
  - Provide device owner with pause / resume / status / quit commands
  - Runs as a simple CLI REPL bound to the local terminal
  - Optionally expose a localhost-only HTTP endpoint (P1)
"""

import threading, logging
log = logging.getLogger("w6")


class LocalControl:
    def __init__(self, agent):
        self.agent = agent

    def start(self):
        """Block the main thread with the REPL."""
        print("\nDormPool Worker — Local Control")
        print("Commands: status | pause | resume | quit\n")
        while self.agent._running:
            try:
                cmd = input("dormpool> ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                self.agent.shutdown()
                break
            if cmd == "pause":
                self.agent.pause()
                print("Paused — no new tasks will be accepted.")
            elif cmd == "resume":
                self.agent.resume()
                print("Resumed.")
            elif cmd == "status":
                mon     = self.agent.monitor
                contrib = self.agent.contrib
                print(f"  contrib : {contrib.state()}")
                print(f"  slots   : {mon.busy_slots()} busy / {mon.free_slots()} free")
                print(f"  cpu     : {mon.cpu_pct()}%")
                print(f"  gpu     : {mon.gpu_pct()}%")
                print(f"  running : {self.agent.exec_mgr.running_task_ids()}")
            elif cmd == "quit":
                self.agent.shutdown()
                break
            elif cmd:
                print("Unknown command. Try: status | pause | resume | quit")
