/** The conformance suite against the in-memory transport pair (`spec/transport-adapter.md`, "Test doubles"). */
import { createPairTarget } from './targets/pair.js';
import { describeConformance } from './vitest.js';

describeConformance(createPairTarget());
