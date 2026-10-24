"""Every corpus scenario this package runs, with its final store checked.

With ``KVGIT_CORPUS_RECORD=1`` the final store is written into the
scenario instead of checked; see ``corpus/README.md``.
"""

import os

import pytest
from corpus_runner import CORPUS, HOST, Run, load, save_final, scenario_paths

RECORD = os.environ.get("KVGIT_CORPUS_RECORD") == "1"


@pytest.mark.parametrize(
    "path", scenario_paths(), ids=lambda p: str(p.relative_to(CORPUS).with_suffix(""))
)
def test_scenario(path):
    scenario = load(path)
    if HOST not in scenario.get("only", [HOST]):
        pytest.skip(f"runs only on {', '.join(scenario['only'])}")
    final = Run(scenario, path).run()
    if RECORD:
        save_final(path, final)
        return
    assert "final" in scenario, f"{path.name} has no final store; record it"
    assert final == scenario["final"]
