import {
  EscrowClient,
  type EscrowEvent,
  type EscrowSnapshot,
  type SorobanEventQuery,
  type SorobanRpcClient,
} from "./index";

const snapshot: EscrowSnapshot = {
  escrow: {
    invoice_id: "INV001",
    admin: "GADMIN",
    sme_address: "GSME",
    amount: "1000000000",
    funding_target: "1000000000",
    funded_amount: "500000000",
    yield_bps: "500",
    maturity: "0",
    status: 1,
  },
  schema_version: 6,
  funding_token: "CTOKEN",
  treasury: "GTREASURY",
  registry: null,
  yield_tiers: null,
  funding_close_snapshot: {
    total_principal: "500000000",
    funding_target: "1000000000",
    closed_at_ledger_timestamp: "1700000000",
    closed_at_ledger_sequence: 42,
  },
  min_contribution_floor: "0",
  max_unique_investors_cap: null,
  max_per_investor_cap: null,
  unique_funder_count: 1,
  legal_hold: false,
  legal_hold_clear_delay: "0",
  legal_hold_clearable_at: null,
  allowlist_active: false,
  primary_attestation_hash: null,
  attestation_log: [],
  collateral: null,
  distributed_principal: "0",
  funding_deadline: null,
  pending_admin: null,
  checksum: "00ff102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
};

const event = (name: string, ledger: number): EscrowEvent => ({
  id: `${name}-${ledger}`,
  type: "contract",
  contract_id: "CESCROW",
  ledger,
  ledger_closed_at: "2026-01-01T00:00:00Z",
  paging_token: `${ledger}-token`,
  topics: [name],
  value: {},
  name,
});

test("streams pages, advances the cursor, and filters event names", async () => {
  const calls: Array<{ cursor?: string; startLedger?: number }> = [];
  const rpc: SorobanRpcClient = {
    invoke: jest.fn(),
    simulate: jest.fn(),
    getLedger: jest.fn().mockResolvedValue({ timestamp: 0, sequence: 42 }),
    getEvents: jest.fn().mockImplementation(async (_filter: unknown, options: SorobanEventQuery) => {
      calls.push(options);
      if (calls.length === 1) {
        return { events: [event("EscrowFunded", 42), event("EscrowSettled", 42)], cursor: "next" };
      }
      return { events: [event("EscrowSettled", 43)], latest_ledger: 43 };
    }),
  };
  const client = new EscrowClient({ rpcUrl: "http://localhost", networkPassphrase: "test", contractId: "CESCROW" }, rpc);
  const controller = new AbortController();
  const stream = client.subscribeEscrowEvents({
    event_names: ["EscrowSettled"],
    poll_interval_ms: 0,
    signal: controller.signal,
  });

  expect((await stream.next()).value?.name).toBe("EscrowSettled");
  expect((await stream.next()).value?.name).toBe("EscrowSettled");
  controller.abort();
  expect(calls).toEqual([
    { startLedger: 42, limit: 100 },
    { cursor: "next", limit: 100 },
  ]);
});

test("fails clearly when getEvents is unavailable", async () => {
  const rpc: SorobanRpcClient = {
    invoke: jest.fn(),
    simulate: jest.fn(),
    getLedger: jest.fn(),
  };
  const client = new EscrowClient({ rpcUrl: "http://localhost", networkPassphrase: "test", contractId: "CESCROW" }, rpc);
  await expect(client.subscribeEscrowEvents().next()).rejects.toThrow("does not support getEvents");
});

test("exports a JSON-safe snapshot payload", async () => {
  const rpc: SorobanRpcClient = {
    invoke: jest.fn(),
    simulate: jest.fn().mockResolvedValue(snapshot),
    getLedger: jest.fn(),
  };
  const client = new EscrowClient({ rpcUrl: "http://localhost", networkPassphrase: "test", contractId: "CESCROW" }, rpc);

  await expect(client.exportState()).resolves.toEqual(snapshot);
  expect(rpc.simulate).toHaveBeenCalledWith("CESCROW", "export_state", []);
});

test("imports a snapshot payload with the contract invocation", async () => {
  const rpc: SorobanRpcClient = {
    invoke: jest.fn().mockResolvedValue(undefined),
    simulate: jest.fn(),
    getLedger: jest.fn(),
  };
  const client = new EscrowClient({ rpcUrl: "http://localhost", networkPassphrase: "test", contractId: "CESCROW" }, rpc);

  await expect(client.importState(snapshot, "GADMIN")).resolves.toBeUndefined();
  expect(rpc.invoke).toHaveBeenCalledWith("CESCROW", "import_state", [snapshot], "GADMIN");
});
