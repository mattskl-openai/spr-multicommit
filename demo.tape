# demo.tape  (run: vhs demo.tape)
Output demo.gif
Set Width 800
Set Height 500
Set FontSize 14
Set Theme "Dracula"
Set TypingSpeed 0.04

# Create a file with some typo, and make commit; tagged with `pr:<tag>`
Type "echo 'some contetn' > pr1.txt"
Enter
Sleep 300ms

Type "git add pr1.txt"
Enter
Sleep 300ms

Type "git commit"
Sleep 300ms
Enter

Type "i"
Type "First PR!"
Enter 2
Type "pr:first"
Sleep 600ms
Ctrl + [
Type ":wq"
Enter
Sleep 300ms

Type "spr list pr"
Sleep 300ms
Enter
Wait
Sleep 1s

# Fix typo and make another commit. No `pr:<tag>`
Type "vim pr1.txt"
Sleep 300ms
Enter
Sleep 200ms

Type@150ms "ww$xxi"
Right
Type "nt"
Ctrl + [
Sleep 500ms
Type ":wq"
Enter

Type 'git commit -a -m "fix typo"'
Sleep 0.3s
Enter
Sleep 0.2s

Type "spr list pr"
Sleep 0.3s
Enter
Wait
Sleep 0.3s

# Create a second PR with another file
Type "echo 'stuff' > pr2.txt"
Enter
Sleep 0.5s

Type 'git add pr2.txt; git commit -m "New file" -m "pr:second"'
Enter
Sleep 0.5s

Type "spr list pr"
Enter
Wait
Sleep 0.5s

# Create pr lits
Type "spr update; # Create PRs for commits"
Sleep 0.5s
Enter
Wait
Sleep 0.5s

Type "spr list pr"
Enter
Wait
Sleep 0.5s

Type "git log --oneline -4;  # Show current commit stack"
Sleep 0.3s
Enter
Wait
Sleep 0.2s

# Land using `flatten`
Type "spr land flatten --until 1; # lands 1 PR"
Sleep 0.5s
Enter
Wait
Sleep 0.5s

Type "spr list pr"
Enter
Wait
Sleep 0.5s

# Restack PRs
Type "spr restack --after 1;  # Restack commits after PR 1"
Sleep 1s
Enter
Wait
Sleep 0.5s

Type "git log --oneline -3;  # Show current commit stack"
Sleep 0.3s
Enter
Wait
Sleep 0.2s

Type "spr list pr"
Enter
Wait
Sleep 5s
