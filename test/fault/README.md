# Deterministic storage fault gate

`make test-faults` runs the storage failure semantics that must remain stable:

- `ENOSPC`, `EROFS`, and `EIO` at the WAL sync boundary poison the writer,
  return an uncertain result, and never advance durable/committed/applied state;
- delayed `fsync` delays acknowledgement;
- short and stalled writes cannot publish a torn artifact;
- an atomic publisher crash before rename does not expose the new pointer;
- torn WAL tails recover only their complete prefix;
- Checkpoint crash points reopen committed data;
- Critical capacity rejects writes while preserving reads and maintenance.

These deterministic tests establish code-level failure semantics. They do not
claim that a particular kernel, filesystem, controller, or SSD reports every
physical failure as the same errno; deployment qualification still requires
device-level fault injection on target hardware.
