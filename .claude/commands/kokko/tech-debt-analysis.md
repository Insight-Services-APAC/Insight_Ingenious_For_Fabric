Perform a deep manual analysis of the entire codebase to identify technical debt through comprehensive code reading and understanding.

**Deep Code Reading & Analysis:**
1. **Read every source file systematically** - understand the complete codebase structure, logic flow, and implementation patterns
2. **Trace execution paths** - follow code from entry points through all major workflows to understand system behavior
3. **Analyze data flow** - understand how data moves through the system, transformations, and storage patterns
4. **Study dependency relationships** - map out how modules, classes, and functions depend on each other

**Architecture & Design Debt:**
1. **Identify architectural inconsistencies** - find places where the code doesn't follow established patterns
2. **Spot violation of SOLID principles** - look for classes with multiple responsibilities, tight coupling, etc.
3. **Find abstraction leaks** - identify where implementation details bleed through interfaces
4. **Discover missing abstractions** - find repeated patterns that should be extracted into reusable components
5. **Analyze layering violations** - find inappropriate cross-layer dependencies

**Code Quality Issues:**
1. **Complex functions/methods** - identify overly long or complex logic that's hard to understand
2. **Poor naming** - find confusing variable, function, or class names that don't express intent
3. **Code duplication** - manually spot repeated logic that should be consolidated
4. **Inconsistent patterns** - find places where similar problems are solved differently
5. **Magic numbers/strings** - identify hardcoded values that should be constants or configuration

**Maintainability Problems:**
1. **Commented-out code** - find dead code left in comments
2. **TODO/FIXME comments** - catalog all deferred work and technical shortcuts
3. **Brittle code** - identify fragile implementations that break easily with changes
4. **Over-engineering** - find unnecessarily complex solutions to simple problems
5. **Under-engineering** - find oversimplified code that will break under load or edge cases

**Business Logic Issues:**
1. **Domain model inconsistencies** - find mismatches between code structure and business concepts
2. **Missing error handling** - identify places where failures aren't properly handled
3. **Incomplete features** - find half-implemented functionality or workarounds
4. **Performance anti-patterns** - spot inefficient algorithms or data access patterns

**Testing & Documentation Gaps:**
1. **Untestable code** - find tightly coupled code that's difficult to test
2. **Missing edge case handling** - identify where the code doesn't handle boundary conditions
3. **Undocumented complex logic** - find intricate business rules without explanation
4. **Inconsistent error messages** - find user-facing errors that are confusing or unhelpful

**Security & Data Handling:**
1. **Input validation gaps** - find places where user input isn't properly sanitized
2. **Information leakage** - identify where sensitive data might be exposed
3. **Authentication/authorization issues** - find inconsistent security checks
4. **Data integrity problems** - spot places where data consistency isn't maintained

**Output Format:**
- **Codebase Overview**: High-level understanding of system architecture and main components
- **Critical Issues**: Most severe technical debt that poses immediate risks
- **Systemic Problems**: Patterns of debt that appear throughout the codebase
- **Module-by-Module Analysis**: Detailed breakdown of issues in each major component
- **Refactoring Opportunities**: Concrete suggestions for improving code quality
- **Risk Assessment**: Impact analysis of identified technical debt
- **Implementation Roadmap**: Prioritized plan for addressing the most important issues

Read the code like a senior developer doing a thorough code review - question every design decision, look for edge cases, and think about long-term maintainability.
