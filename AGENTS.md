
# Codex Working Instructions

## Development workflow

- Follow the existing architecture and coding conventions.
- Keep changes focused on the requested task.
- Run relevant tests and report the actual results.
- Do not invent test results or claim unverified functionality.
- Do not commit, push, create pull requests, or merge without explicit approval.
- Never expose secrets or credentials.

## Completion report

After completing an implementation task, provide:

1. Summary of changes and affected files.
2. Tests executed and their actual results.
3. Remaining risks or unverified behavior.
4. A suggested Git commit message.
5. A suggested GitHub Pull Request title and description.

Only prepare commit and PR suggestions when the change is ready for review. Do not automatically perform Git operations.

## Git commit conventions

- Use concise English commit messages.
- Prefer imperative verbs, such as Add, Fix, Refactor, or Update.
- Describe the actual change, not generic phrases.

## Pull Request format

Provide the PR title separately as plain text.

Provide the PR description in a standalone Markdown code block, ready to copy into GitHub.

Use this structure:

## Summary
- Explain what changed and why.
- Keep the description concise.

## Testing
- List only tests actually executed.
- Include pass/fail results or explain what was not run.

## Notes
- Mention important limitations or risks only when relevant.

Do not include the PR title inside the Markdown description.
Do not include Markdown code-fence delimiters in the content intended for pasting into GitHub.
