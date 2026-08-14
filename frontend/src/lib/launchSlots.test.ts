import { describe, expect, it } from 'vitest';
import { MAX_GLOBAL_WORKER_SLOTS, launchManagerSlots, launchWorkerSlots } from './launchSlots';

describe('launchManagerSlots', () => {
  it('lets every planned manager execute', () => {
    // Workers only run inside an executing workstream, so a cap of 2 against 5
    // managers idled three whole workstreams worth of coders.
    expect(launchManagerSlots(5, 2)).toBe(5);
    expect(launchManagerSlots(4, 4)).toBe(4);
  });

  it('never exceeds the number of managers that exist', () => {
    expect(launchManagerSlots(2, 16)).toBe(2);
  });

  it('always allows at least one', () => {
    expect(launchManagerSlots(0, 0)).toBe(1);
  });
});

describe('launchWorkerSlots', () => {
  it('widens the slot count to match the fan-out the wizard configured', () => {
    // Five managers of four coders is twenty coders; a leftover default of 8
    // used to run a quarter of them and queue the rest.
    expect(launchWorkerSlots(5, 5, 8)).toBe(20);
    expect(launchWorkerSlots(4, 5, 8)).toBe(16);
  });

  it('never lowers a slot count the operator raised on purpose', () => {
    expect(launchWorkerSlots(2, 3, 40)).toBe(40);
  });

  it('stays within the backend ceiling', () => {
    expect(launchWorkerSlots(32, 32, 8)).toBe(MAX_GLOBAL_WORKER_SLOTS);
  });

  it('handles a single coder per manager', () => {
    expect(launchWorkerSlots(3, 2, 1)).toBe(3);
  });
});
