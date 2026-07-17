#!/usr/bin/env python3
"""
🤖 Orchestrator Loop - Lance les agents Cursor en boucle

Ce script tourne en continu et écrit des "triggers" pour que l'IA Cursor
lance les agents à chaque itération.

Les agents Cursor (Analyst, Data, Builder) sont lancés via mcp_task
uniquement depuis Cursor - ce script coordonne le cycle.

Usage:
    python orchestrator_loop.py [--iterations 5] [--interval 60]

Le script:
1. Écrit agent_status.md avec l'état actuel
2. Écrit ORCHESTRATOR_TRIGGER.json pour signaler "lance les agents"
3. Attend que les agents aient fini (via AGENTS_DONE.json)
4. Répète
"""

import json
import time
import argparse
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
TRIGGER_FILE = PROJECT_ROOT / "ORCHESTRATOR_TRIGGER.json"
DONE_FILE = PROJECT_ROOT / "AGENTS_DONE.json"
STATUS_FILE = PROJECT_ROOT / "agent_status.md"
WORK_LOG = PROJECT_ROOT / "WORK_LOG.md"


def write_trigger(iteration: int):
    """Écrit le fichier trigger pour que Cursor lance les agents"""
    data = {
        "action": "launch_agents",
        "iteration": iteration,
        "timestamp": datetime.now().isoformat(),
        "agents": ["analyst", "data", "builder"],
    }
    TRIGGER_FILE.write_text(json.dumps(data, indent=2))
    print(f"  → Trigger écrit (itération {iteration})")


def wait_for_agents(timeout_seconds: int = 600):
    """Attend que les agents aient fini (AGENTS_DONE.json créé)"""
    start = time.time()
    while (time.time() - start) < timeout_seconds:
        if DONE_FILE.exists():
            try:
                data = json.loads(DONE_FILE.read_text())
                print(f"  ✓ Agents terminés: {data.get('summary', 'OK')}")
                DONE_FILE.unlink()  # Remove so next iteration can trigger
                return True
            except Exception:
                pass
        time.sleep(5)
    print("  ⚠ Timeout - agents pas terminés")
    return False


def update_status(iteration: int, message: str):
    """Met à jour agent_status.md"""
    status = f"""# 🤖 Multi-Agent Monitor

**Orchestrator:** 🟢 Running (boucle)
**Iteration:** {iteration}
**Dernière mise à jour:** {datetime.now().strftime('%H:%M:%S')}

---

## Activité

{message}

---

## Instructions pour Cursor

Quand ORCHESTRATOR_TRIGGER.json existe:
1. Lance les 3 agents (Analyst, Data, Builder) via mcp_task
2. Quand ils ont fini, crée AGENTS_DONE.json avec un résumé
3. Supprime ORCHESTRATOR_TRIGGER.json (ou le script le fait)

"""
    STATUS_FILE.write_text(status)


def append_work_log(entry: str):
    """Ajoute une entrée au WORK_LOG"""
    if WORK_LOG.exists():
        content = WORK_LOG.read_text()
    else:
        content = "# Work Log\n\n"
    content += f"\n### [{datetime.now().strftime('%H:%M:%S')}] {entry}\n"
    WORK_LOG.write_text(content)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", "-i", type=int, default=999, help="Nombre d'itérations (999 = infini)")
    parser.add_argument("--interval", "-t", type=int, default=120, help="Secondes entre chaque itération")
    args = parser.parse_args()

    print("""
╔═══════════════════════════════════════════════════════════╗
║  🤖 Orchestrator Loop - Agents Cursor en boucle           ║
║                                                           ║
║  À chaque itération, le trigger ORCHESTRATOR_TRIGGER.json ║
║  est écrit. L'IA Cursor doit lancer les agents.           ║
║                                                           ║
║  Pour que ça marche: garde cette conversation ouverte    ║
║  et dis "lance les agents" quand tu vois le trigger.      ║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
""")

    iteration = 0
    try:
        while iteration < args.iterations:
            iteration += 1
            print(f"\n{'='*50}")
            print(f"  ITÉRATION {iteration}")
            print(f"{'='*50}")

            update_status(iteration, f"En attente du lancement des agents (itération {iteration})...")

            # Écrire le trigger
            write_trigger(iteration)
            append_work_log(f"Orchestrator: trigger itération {iteration} écrit")

            # Attendre que les agents finissent
            print(f"  Attente des agents (timeout {600}s)...")
            done = wait_for_agents(600)

            if done:
                update_status(iteration, f"Itération {iteration} terminée. Prochaine dans {args.interval}s.")
                append_work_log(f"Orchestrator: itération {iteration} complétée par les agents")
            else:
                update_status(iteration, f"Timeout itération {iteration}. Réessai dans {args.interval}s.")

            if iteration < args.iterations:
                print(f"\n  Prochaine itération dans {args.interval}s... (Ctrl+C pour arrêter)")
                time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n\n[yellow]Orchestrator arrêté par l'utilisateur[/yellow]")
        update_status(iteration, "Arrêté par l'utilisateur")
    finally:
        if TRIGGER_FILE.exists():
            TRIGGER_FILE.unlink()
        print(f"\nTotal: {iteration} itérations exécutées")


if __name__ == "__main__":
    main()
