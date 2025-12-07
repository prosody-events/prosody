---

description: "Task list template for feature implementation"
---

# Tasks: [FEATURE NAME]

**Input**: Design documents from `/specs/[###-feature-name]/`
**Prerequisites**: plan.md (required), spec.md (required for scenarios), research.md, data-model.md, contracts/

**Tests**: The examples below include test tasks. Tests are OPTIONAL - only include them if explicitly requested in the feature specification.

**Organization**: Tasks are grouped by scenario to enable independent implementation and testing of each scenario.

## Format: `[ID] [P?] [S#] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[S#]**: Which scenario this task belongs to (e.g., S1, S2, S3)
- Include exact file paths in descriptions

## Path Conventions

- **Single project**: `src/`, `tests/` at repository root
- **Web app**: `backend/src/`, `frontend/src/`
- **Mobile**: `api/src/`, `ios/src/` or `android/src/`
- Paths shown below assume single project - adjust based on plan.md structure

<!--
  ============================================================================
  IMPORTANT: The tasks below are SAMPLE TASKS for illustration purposes only.

  The /speckit.tasks command MUST replace these with actual tasks based on:
  - Scenarios from spec.md (with their priorities P1, P2, P3...)
  - Feature requirements from plan.md
  - Entities from data-model.md
  - Endpoints from contracts/

  Tasks MUST be organized by scenario so each scenario can be:
  - Implemented independently
  - Tested independently
  - Delivered as an MVP increment

  DO NOT keep these sample tasks in the generated tasks.md file.
  ============================================================================
-->

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [ ] T001 Create project structure per implementation plan
- [ ] T002 Initialize [language] project with [framework] dependencies
- [ ] T003 [P] Configure linting and formatting tools

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY scenario can be implemented

**⚠️ CRITICAL**: No scenario work can begin until this phase is complete

Examples of foundational tasks (adjust based on your project):

- [ ] T004 Setup database schema and migrations framework
- [ ] T005 [P] Implement authentication/authorization framework
- [ ] T006 [P] Setup API routing and middleware structure
- [ ] T007 Create base models/entities that all scenarios depend on
- [ ] T008 Configure error handling and logging infrastructure
- [ ] T009 Setup environment configuration management

**Checkpoint**: Foundation ready - scenario implementation can now begin in parallel

---

## Phase 3: Scenario 1 - [Title] (Priority: P1) 🎯 MVP

**Goal**: [Brief description of what this scenario delivers]

**Independent Test**: [How to verify this scenario works on its own]

### Tests for Scenario 1 (OPTIONAL - only if tests requested) ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T010 [P] [S1] Contract test for [endpoint] in tests/contract/test_[name].py
- [ ] T011 [P] [S1] Integration test for [scenario] in tests/integration/test_[name].py

### Implementation for Scenario 1

- [ ] T012 [P] [S1] Create [Entity1] model in src/models/[entity1].py
- [ ] T013 [P] [S1] Create [Entity2] model in src/models/[entity2].py
- [ ] T014 [S1] Implement [Service] in src/services/[service].py (depends on T012, T013)
- [ ] T015 [S1] Implement [endpoint/feature] in src/[location]/[file].py
- [ ] T016 [S1] Add validation and error handling
- [ ] T017 [S1] Add logging for scenario 1 operations

**Checkpoint**: At this point, Scenario 1 should be fully functional and testable independently

---

## Phase 4: Scenario 2 - [Title] (Priority: P2)

**Goal**: [Brief description of what this scenario delivers]

**Independent Test**: [How to verify this scenario works on its own]

### Tests for Scenario 2 (OPTIONAL - only if tests requested) ⚠️

- [ ] T018 [P] [S2] Contract test for [endpoint] in tests/contract/test_[name].py
- [ ] T019 [P] [S2] Integration test for [scenario] in tests/integration/test_[name].py

### Implementation for Scenario 2

- [ ] T020 [P] [S2] Create [Entity] model in src/models/[entity].py
- [ ] T021 [S2] Implement [Service] in src/services/[service].py
- [ ] T022 [S2] Implement [endpoint/feature] in src/[location]/[file].py
- [ ] T023 [S2] Integrate with Scenario 1 components (if needed)

**Checkpoint**: At this point, Scenarios 1 AND 2 should both work independently

---

## Phase 5: Scenario 3 - [Title] (Priority: P3)

**Goal**: [Brief description of what this scenario delivers]

**Independent Test**: [How to verify this scenario works on its own]

### Tests for Scenario 3 (OPTIONAL - only if tests requested) ⚠️

- [ ] T024 [P] [S3] Contract test for [endpoint] in tests/contract/test_[name].py
- [ ] T025 [P] [S3] Integration test for [scenario] in tests/integration/test_[name].py

### Implementation for Scenario 3

- [ ] T026 [P] [S3] Create [Entity] model in src/models/[entity].py
- [ ] T027 [S3] Implement [Service] in src/services/[service].py
- [ ] T028 [S3] Implement [endpoint/feature] in src/[location]/[file].py

**Checkpoint**: All scenarios should now be independently functional

---

[Add more scenario phases as needed, following the same pattern]

---

## Phase N: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple scenarios

- [ ] TXXX [P] Documentation updates in docs/
- [ ] TXXX Code cleanup and refactoring
- [ ] TXXX Performance optimization across all scenarios
- [ ] TXXX [P] Additional unit tests (if requested) in tests/unit/
- [ ] TXXX Security hardening
- [ ] TXXX Run quickstart.md validation

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all scenarios
- **Scenarios (Phase 3+)**: All depend on Foundational phase completion
  - Scenarios can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3)
- **Polish (Final Phase)**: Depends on all desired scenarios being complete

### Scenario Dependencies

- **Scenario 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other scenarios
- **Scenario 2 (P2)**: Can start after Foundational (Phase 2) - May integrate with S1 but should be independently testable
- **Scenario 3 (P3)**: Can start after Foundational (Phase 2) - May integrate with S1/S2 but should be independently testable

### Within Each Scenario

- Tests (if included) MUST be written and FAIL before implementation
- Models before services
- Services before endpoints
- Core implementation before integration
- Scenario complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- All Foundational tasks marked [P] can run in parallel (within Phase 2)
- Once Foundational phase completes, all scenarios can start in parallel (if team capacity allows)
- All tests for a scenario marked [P] can run in parallel
- Models within a scenario marked [P] can run in parallel
- Different scenarios can be worked on in parallel by different team members

---

## Parallel Example: Scenario 1

```bash
# Launch all tests for Scenario 1 together (if tests requested):
Task: "Contract test for [endpoint] in tests/contract/test_[name].py"
Task: "Integration test for [scenario] in tests/integration/test_[name].py"

# Launch all models for Scenario 1 together:
Task: "Create [Entity1] model in src/models/[entity1].py"
Task: "Create [Entity2] model in src/models/[entity2].py"
```

---

## Implementation Strategy

### MVP First (Scenario 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all scenarios)
3. Complete Phase 3: Scenario 1
4. **STOP and VALIDATE**: Test Scenario 1 independently
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add Scenario 1 → Test independently → Deploy/Demo (MVP!)
3. Add Scenario 2 → Test independently → Deploy/Demo
4. Add Scenario 3 → Test independently → Deploy/Demo
5. Each scenario adds value without breaking previous scenarios

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: Scenario 1
   - Developer B: Scenario 2
   - Developer C: Scenario 3
3. Scenarios complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [S#] label maps task to specific scenario for traceability
- Each scenario should be independently completable and testable
- Verify tests fail before implementing
- Commit after each task or logical group
- Stop at any checkpoint to validate scenario independently
- Avoid: vague tasks, same file conflicts, cross-scenario dependencies that break independence
