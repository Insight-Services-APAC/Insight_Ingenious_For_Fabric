# Prompt: Pull Changes from Main Branch and Resolve Merge Conflicts

## Instructions

1. **Fetch the latest changes from the remote repository**
    ```sh
    git fetch origin
    ```

2. **Switch to main branch and pull latest changes**
    ```sh
    git checkout main
    git pull origin main
    ```

3. **Switch back to current working branch**
    ```sh
    git checkout -
    ```

4. **Merge or rebase main into current branch**
    Choose one of the following strategies:

    **Option A: Merge (preserves commit history)**
    ```sh
    git merge main
    ```

    **Option B: Rebase (cleaner linear history)**
    ```sh
    git rebase main
    ```

5. **If merge conflicts occur:**
    - Identify conflicted files: `git status`
    - Open each conflicted file and resolve conflicts manually
    - Look for conflict markers: `<<<<<<<`, `=======`, `>>>>>>>`
    - Edit files to resolve conflicts, keeping the desired changes
    - Stage resolved files: `git add <resolved-file>`
    - Continue the merge/rebase:
        - For merge: `git commit` (or `git merge --continue`)
        - For rebase: `git rebase --continue`

6. **Verify the merge/rebase was successful**
    ```sh
    git status
    git log --oneline -10
    ```

7. **Run tests to ensure everything still works**
    ```sh
    uv run pytest
    uv run pre-commit run --all-files
    ```

8. **Push the updated branch**
    ```sh
    git push origin <current-branch-name>
    ```

## Conflict Resolution Tips

- **Understanding conflict markers:**
    - `<<<<<<< HEAD` - Your current branch changes
    - `=======` - Separator
    - `>>>>>>> main` - Main branch changes

- **Common resolution strategies:**
    - Keep both changes (merge them logically)
    - Keep only main branch changes
    - Keep only current branch changes
    - Create a new solution that combines the best of both

- **Tools to help:**
    - `git diff` - See differences
    - `git log --oneline main..HEAD` - See commits unique to current branch
    - `git log --oneline HEAD..main` - See commits unique to main

## Example Workflow

```sh
# Fetch latest changes
git fetch origin

# Update main branch
git checkout main
git pull origin main

# Go back to feature branch
git checkout feature/my-feature

# Merge main into feature branch
git merge main

# If conflicts occur, resolve them then:
git add .
git commit -m "Resolve merge conflicts with main"

# Run tests
uv run pytest

# Push updated branch
git push origin feature/my-feature
