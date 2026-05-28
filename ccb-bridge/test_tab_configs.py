"""Test tab config functions."""
import sys
import os
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "lib"))
from bus_client import generate_tab_config, install_tab_configs, list_tab_configs, _templates_dir, _warp_tab_configs_dir

print("=== Templates dir ===")
print(_templates_dir())
print(os.listdir(_templates_dir()))

print()
print("=== Warp tab_configs dir ===")
td = _warp_tab_configs_dir()
print(td)

print()
print("=== Test generate_tab_config ===")
toml = generate_tab_config(
    "Test Gen",
    [
        dict(id="root", split="vertical", children=["a", "b"]),
        dict(id="a", type="terminal", directory="{{cwd}}", is_focused=True, commands=[]),
        dict(id="b", type="terminal", directory="{{cwd}}"),
    ],
    title="Generated Test",
    color="cyan",
    params=dict(cwd=dict(type="text", description="Working dir")),
    write=False,
)
print(toml)

print()
print("=== Install tab configs ===")
test_dir = tempfile.mkdtemp(prefix="test_tab_configs_")
os.environ["WARP_CCB_TAB_CONFIGS_DIR"] = test_dir
installed = install_tab_configs()
print("Installed:", installed)
assert len(installed) == 3, f"Expected 3 templates, got {len(installed)}"

print()
print("=== List tab configs ===")
configs = list_tab_configs()
for c in configs:
    print("  {} ({}) color={} title={}".format(
        c.get("name", "?"), c.get("filename", "?"),
        c.get("color", "-"), c.get("title", "-")))

assert len(configs) == 3, f"Expected 3 configs, got {len(configs)}"

print()
print("=== Verify TOML content ===")
for c in configs:
    with open(c["path"], "r") as f:
        content = f.read()
    has_root = "split = " in content
    has_children = "children = [" in content
    has_terminal = 'type = "terminal"' in content
    has_params = "[params." in content
    has_commands_array = "commands = [" in content
    fname = c.get("filename", "?")
    print("  {}: root={} children={} terminal={} params={} no_commands={}".format(
        fname, has_root, has_children, has_terminal, has_params, not has_commands_array))
    assert has_root, f"{fname} missing split"
    assert has_children, f"{fname} missing children"
    assert has_terminal, f"{fname} missing terminal type"
    assert has_params, f"{fname} missing params"
    assert not has_commands_array, f"{fname} should not have commands array"

print()
print("=== Verify generate_tab_config writes to disk ===")
gen_toml = generate_tab_config(
    "Generated Config",
    [
        dict(id="root", split="horizontal", children=["top", "bottom"]),
        dict(id="top", type="agent", directory="{{cwd}}", is_focused=True),
        dict(id="bottom", type="terminal", directory="{{cwd}}"),
    ],
    color="green",
    write=True,
)
gen_path = os.path.join(test_dir, "generated_config.toml")
assert os.path.exists(gen_path), "generate_tab_config did not write file"
with open(gen_path) as f:
    disk_content = f.read()
assert gen_toml == disk_content, "Written content mismatch"
print("  Written to:", gen_path)

print()
print("=== Verify install_tab_configs skip existing ===")
installed2 = install_tab_configs(force=False)
print("  Re-install (no force):", installed2)
assert len(installed2) == 0, "Should skip existing files without force"

installed3 = install_tab_configs(force=True)
print("  Re-install (force):", installed3)
assert len(installed3) == 3, "Should overwrite with force"

print()
print("All tests passed!")
