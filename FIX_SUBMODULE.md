# Fix Submodule Push Issue

## The Problem
The main rose-db repository references commits in the storage-engine submodule that don't exist on GitHub yet. This causes the error:
```
fatal: Fetched in submodule path 'storage-engine', but it did not contain ff2ac3ca...
```

## The Solution

Run these commands from your **local machine** (not CI):

### Step 1: Clone the repo with submodules
```bash
git clone https://github.com/mattcuento/rose-db.git
cd rose-db
git checkout claude/query-dataframe-api-vcLnL
```

### Step 2: Initialize submodules
```bash
git submodule update --init --recursive
```

### Step 3: Go into storage-engine and push the commits
```bash
cd storage-engine

# Check current status
git log --oneline -5
# Should show:
# ff2ac3c Add iteration support to TableHeap for query engine
# 6ade44c Add B+ tree index implementation with latch crabbing

# Create a branch from current commit
git checkout -b feature/query-engine-integration

# Push to storage-engine repository
git push -u origin feature/query-engine-integration
```

### Step 4: Update .gitmodules to use the branch (optional)
```bash
cd ..  # Back to rose-db root

# Edit .gitmodules to specify branch (optional but recommended)
# Add: branch = feature/query-engine-integration
# under [submodule "storage-engine"]

# Commit the change
git add .gitmodules
git commit -m "Update storage-engine submodule to use feature branch"
git push
```

### Step 5: Alternatively, merge storage-engine to main
If you want to keep it simple:

```bash
cd storage-engine
git checkout master  # or main
git merge feature/query-engine-integration
git push origin master
cd ..
git submodule update --remote
git add storage-engine
git commit -m "Update storage-engine submodule reference"
git push
```

## Verification

After pushing, anyone can now clone:
```bash
git clone --recursive https://github.com/mattcuento/rose-db.git
cd rose-db
ls storage-engine/src/index/  # Should show B+ tree files!
```

## Why This Happened

When working with submodules:
1. Commits in the submodule must be pushed to the submodule's repository FIRST
2. THEN the parent repository can reference those commits
3. We created the commits locally but never pushed them to GitHub

This is a common Git submodule workflow issue - always remember to push submodule commits before pushing the parent!

## Quick Fix for Others

Until you push the storage-engine commits, others can work around this by:
1. Cloning without submodules initially
2. Manually cloning storage-engine
3. Checking out the right commit

But the proper fix is to push the storage-engine commits as shown above.
