"""
Full DQ Pipeline Orchestrator
-----------------------------
Purpose:
    Provides one deployment entry point to run the full pipeline in sequence:

        1. Predifined_Rule_Checks.py
        2. LLM_Rule_Checks.py

Key design:
    - Predifined_Rule_Checks.py returns the exact CSV path it created.
    - LLM_Rule_Checks.py consumes that exact CSV path.
    - No glob/latest-file logic is used for handoff.
    - Both scripts share the same RUN_ID for traceability.

Deployment team should run:
    python run_full_pipeline.py
"""

from datetime import datetime
from typing import Optional
import sys
import traceback
import json
from Predifined_Rule_Checks import main as run_predefined_rules
from LLM_Rule_Checks import main as run_llm_rules
import os

LOG_DIR = "Log"


class TeeStream:
    """Write output to both the console and the execution log file."""
    def __init__(self, console_stream, log_stream):
        self.console_stream = console_stream
        self.log_stream = log_stream

    def write(self, message):
        self.console_stream.write(message)
        self.log_stream.write(message)
        self.log_stream.flush()

    def flush(self):
        self.console_stream.flush()
        self.log_stream.flush()

    def isatty(self):
        return False


def run_with_logging(run_id: Optional[str] = None) -> None:
    """Run the existing pipeline while saving stdout, stderr, and tracebacks."""
    if run_id is None:
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f"Run_Pipeline_{run_id}.log")
    original_stdout, original_stderr = sys.stdout, sys.stderr
    with open(log_file, "w", encoding="utf-8", buffering=1) as log_stream:
        sys.stdout = TeeStream(original_stdout, log_stream)
        sys.stderr = TeeStream(original_stderr, log_stream)
        try:
            print(f"Execution log: {os.path.abspath(log_file)}")
            main(run_id=run_id)
        except Exception:
            print("\nUNHANDLED PIPELINE ERROR")
            traceback.print_exc()
            raise
        finally:
            sys.stdout, sys.stderr = original_stdout, original_stderr


def write_pipeline_context(
    run_id: str,
    mandatory_file: str,
    ai_file: str
) -> str:
    """
    Creates pipeline_context.json used by UI.py.
    """

    context = {
        "run_id": run_id,
        "mandatory_file": mandatory_file,
        "ai_file": ai_file
    }

    context_file = "pipeline_context.json"

    with open(context_file, "w", encoding="utf-8") as f:
        json.dump(context, f, indent=4)

    print(f"Pipeline context created: {os.path.abspath(context_file)}")

    return context_file
def main(run_id: Optional[str] = None) -> None:
    """
    Definition:
        Runs predefined rules first and then passes its output CSV directly to
        the LLM rule-generation script.

    Input:
        run_id: Optional run id. If not supplied, it is generated once and reused
        across both pipeline stages.

    Output:
        None. Prints generated artifact paths.
    """
    if run_id is None:
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 80)
    print(f"FULL DQ PIPELINE STARTED | RUN_ID={run_id}")
    print("=" * 80)

    print("\nSTEP 1: Running predefined mandatory DQ checks")
    predefined_output_file = run_predefined_rules(run_id=run_id)

    if not predefined_output_file:
        raise RuntimeError("Predefined rule pipeline did not create any CSV output. LLM stage cannot continue.")

    print(f"Predefined output file: {predefined_output_file}")

    print("\nSTEP 2: Running LLM-generated DQ checks")
    llm_output_file = run_llm_rules(input_file=predefined_output_file, run_id=run_id)

    print(f"LLM output file: {llm_output_file}")
    
    print("\nSTEP 3: Creating Pipeline Context File")
    write_pipeline_context(run_id=run_id, 
                           mandatory_file=predefined_output_file, 
                           ai_file=llm_output_file)
    print("\n" + "=" * 80)
    print(f"FULL DQ PIPELINE COMPLETED | RUN_ID={run_id}")
    print("=" * 80)


if __name__ == "__main__":
    run_with_logging()
