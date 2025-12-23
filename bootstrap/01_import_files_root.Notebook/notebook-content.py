# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# ---------------------------------------------
# Fabric Quickstart (workspace-agnostic, ABFS-based)
# - Create (or reuse) Lakehouse in the *current* workspace
# - Import ALL text assets from GitHub BRANCH ROOT into ABFS .../Files/quickstart
# - Preserve subfolders; exclude 'bootstrap' by default
# ---------------------------------------------

import notebookutils as nb
import urllib.request, json, os

# ---- Configure your repo ----
GITHUB_OWNER  = "bcgov"
GITHUB_REPO   = "nr-dap-azure"
GITHUB_BRANCH = "fabric-lakehouse-medallion-quickstart"  # branch whose *root* you want to copy

# Optional: GitHub token to avoid rate limits (fine-grained or classic, read-only)
GITHUB_TOKEN = None  # e.g., "ghp_XXXXXXXXXXXXXXXXXXXXXXXXXXXX"

# ---- Lakehouse settings ----
LAKEHOUSE_NAME = "lh_sales_core"
LAKEHOUSE_DESC = "Lakehouse for Fabric Medallion Quickstart"

# ---- File filters ----
# Treat these extensions as TEXT (copied via nb.fs.put). Others are skipped unless COPY_BINARIES=True.
TEXT_EXTS = {".csv", ".tsv", ".json", ".md", ".txt", ".py", ".sql", ".yml", ".yaml"}

# ✅ Exclude the committed notebook folder by default
EXCLUDE_DIRS = {"bootstrap"}  # add more top-level folders here if needed

# Toggle if you also want to bring binaries (images, zip, parquet parts) via fs.cp
COPY_BINARIES = False

# ---------------------------------------------
# Helpers
# ---------------------------------------------
def _headers():
    return {"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}

def _http_json(url: str):
    req = urllib.request.Request(url, headers=_headers())
    with urllib.request.urlopen(req) as resp:
        if resp.status != 200:
            raise RuntimeError(f"GET {url} -> HTTP {resp.status}")
        return json.loads(resp.read().decode("utf-8"))

def _http_text(url: str) -> str:
    req = urllib.request.Request(url, headers=_headers())
    with urllib.request.urlopen(req) as resp:
        if resp.status != 200:
            raise RuntimeError(f"GET {url} -> HTTP {resp.status}")
        return resp.read().decode("utf-8")

def list_branch_tree(owner: str, repo: str, branch: str):
    # GitHub Trees API: recursive listing of files at branch tip
    url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"
    data = _http_json(url)
    return [item for item in data.get("tree", []) if item.get("type") == "blob"]

def raw_url(owner: str, repo: str, branch: str, path: str) -> str:
    return f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"

def is_text_file(path: str) -> bool:
    _, ext = os.path.splitext(path)
    return ext.lower() in TEXT_EXTS

# ---------------------------------------------
# 1) Resolve current workspace + create Lakehouse *here*
# ---------------------------------------------
WORKSPACE_ID = spark.conf.get("trident.workspace.id")
print("WorkspaceId:", WORKSPACE_ID)
print("Lakehouse name:", LAKEHOUSE_NAME)

try:
    nb.lakehouse.create(name=LAKEHOUSE_NAME, description=LAKEHOUSE_DESC, workspaceId=WORKSPACE_ID)
    print("Created Lakehouse (or it already existed).")
except Exception as e:
    print("Lakehouse may already exist:", e)

# 2) Resolve the ABFS path of *this* Lakehouse in *this* workspace
lh_props   = nb.lakehouse.getWithProperties(name=LAKEHOUSE_NAME, workspaceId=WORKSPACE_ID)
ABFS_ROOT  = lh_props["properties"]["abfsPath"]  # e.g., abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lakehouse>
DEST_DIR   = f"{ABFS_ROOT}/Files/quickstart"
print("ABFS root:", ABFS_ROOT)
print("Destination (ABFS):", DEST_DIR)

# Ensure the top-level quickstart folder exists
nb.fs.mkdirs(DEST_DIR)  # ABFS-safe; no friendly-name resolution needed

# ---------------------------------------------
# 3) Copy branch-root files/folders into ABFS .../Files/quickstart
# ---------------------------------------------
print("\nListing branch tree (this may take a moment)...")
tree = list_branch_tree(GITHUB_OWNER, GITHUB_REPO, GITHUB_BRANCH)

copied_text, copied_bin, skipped = 0, 0, 0
for item in tree:
    repo_path = item["path"]  # e.g., "docs/readme.md" or "users.tsv"

    # Skip excluded top-level directories
    top = repo_path.split("/", 1)[0]
    if top in EXCLUDE_DIRS:
        skipped += 1
        continue

    # Compute destination folder under ABFS (preserve repo subfolders)
    dest_dir = f"{DEST_DIR}/{os.path.dirname(repo_path)}" if "/" in repo_path else DEST_DIR
    try:
        nb.fs.mkdirs(dest_dir)
    except Exception as e:
        print(f"mkdirs({dest_dir}) -> {e}")

    # Decide copy method
    if is_text_file(repo_path):
        # Text: use nb.fs.put (writes UTF-8 text)
        try:
            text = _http_text(raw_url(GITHUB_OWNER, GITHUB_REPO, GITHUB_BRANCH, repo_path))
            nb.fs.put(f"{dest_dir}/{os.path.basename(repo_path)}", text, overwrite=True)
            copied_text += 1
            print(f"Text copied: {repo_path}  ->  {dest_dir}/{os.path.basename(repo_path)}")
        except Exception as e:
            skipped += 1
            print(f"Skip text {repo_path}: {e}")
    else:
        # Binary: optionally copy via fs.cp (HTTP -> ABFS)
        if COPY_BINARIES:
            try:
                src = raw_url(GITHUB_OWNER, GITHUB_REPO, GITHUB_BRANCH, repo_path)
                dst = f"{dest_dir}/{os.path.basename(repo_path)}"
                nb.fs.cp(src, dst, recurse=False)  # direct copy across filesystems
                copied_bin += 1
                print(f"Binary copied: {repo_path}  ->  {dst}")
            except Exception as e:
                skipped += 1
                print(f"Skip binary {repo_path}: {e}")
        else:
            skipped += 1

print(f"\n✅ Done. {copied_text} text file(s) and {copied_bin} binary file(s) imported into {DEST_DIR}. Skipped {skipped}.")
print("Explore them under Lakehouse → Files → quickstart in the left pane.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
