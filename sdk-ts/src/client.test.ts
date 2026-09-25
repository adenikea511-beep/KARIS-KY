import {
  EscrowClient,
  type EscrowEvent,
  type SorobanEventQuery,
  type SorobanRpcClient,
  ValidationError,
} from "./index";

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
  const client = new EscrowClient({ rpcUrl: "http://localhost", networkPassphrase: "test" }, rpc);
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
  const client = new EscrowClient({ rpcUrl: "http://localhost", networkPassphrase: "test" }, rpc);
  await expect(client.subscribeEscrowEvents().next()).rejects.toThrow("does not support getEvents");
});

test("appendAttestationDigest invokes the contract with a 32-byte digest", async () => {
  const rpc: SorobanRpcClient = {
    invoke: jest.fn().mockResolvedValue(undefined),
    simulate: jest.fn(),
    getLedger: jest.fn(),
  };
  const client = new EscrowClient({ rpcUrl: "http://localhost", networkPassphrase: "test" }, rpc);
  const digest = new Uint8Array(32).fill(7);

  await client.appendAttestationDigest(digest);

  expect(rpc.invoke).toHaveBeenCalledWith("", "append_attestation_digest", [digest], undefined);
});

test("appendAttestationDigest rejects invalid digest lengths before invoking RPC", async () => {
  const rpc: SorobanRpcClient = {
    invoke: jest.fn(),
    simulate: jest.fn(),
    getLedger: jest.fn(),
  };
  const client = new EscrowClient({ rpcUrl: "http://localhost", networkPassphrase: "test" }, rpc);

  await expect(client.appendAttestationDigest(new Uint8Array(31))).rejects.toBeInstanceOf(ValidationError);
  expect(rpc.invoke).not.toHaveBeenCalled();
});

test("appendAttestationDigest passes rate-limit errors through unchanged", async () => {
  const rateLimitError = new Error("rate limit exceeded");
  const rpc: SorobanRpcClient = {
    invoke: jest.fn().mockRejectedValue(rateLimitError),
    simulate: jest.fn(),
    getLedger: jest.fn(),
  };
  const client = new EscrowClient({ rpcUrl: "http://localhost", networkPassphrase: "test" }, rpc);

  await expect(client.appendAttestationDigest(new Uint8Array(32))).rejects.toBe(rateLimitError);
});
