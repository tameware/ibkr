# Intended audience

These instructions have been created to help Cursor but are expected to be useful more broadly.

# Logs

Latest logs can be found at /Users/adamw/mnt/sideswap/src/ibkr/logs.
Always let me know if this directory is not accessible.

# Test-driven development

For every code change, follow strict TDD:

1. Write or update automated tests that initially fail and describe the desired behavior and edge cases.
2. Write the minimal production code needed to make the new tests pass. Do not add untested functionality.
3. After tests are green, refactor tests and implementation for clarity and to remove duplication.
4. Run the relevant tests after each refactor and keep them green.
5. Repeat this red-green-refactor cycle in small steps.

Structure tests with Arrange-Act-Assert and use descriptive names that capture intent, including negative and boundary cases.

Never write production code first and wrap tests around it afterward. Tests must be written first and must fail before implementation.

Tests are not necessary for changes to documentation, unless the change is coupled to a code or configuration change.
