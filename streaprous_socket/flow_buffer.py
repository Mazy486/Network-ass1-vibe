import time
import threading

try:
    from . import constant as constants
except ImportError:
    import constant as constants


class SendBufferManager:
    """Manages send buffer, in-flight accounting, and retransmissions."""

    def __init__(
        self,
        initial_seq=0,
        mss=constants.MSS,
        max_segments=constants.SND_WIN_SIZE,
        rwnd=None,
        rto=constants.INITIAL_RTO,
        duplicate_ack_threshold=constants.DUP_ACK_THRESHOLD,
        win_size=None,
    ):
        if win_size is not None:
            max_segments = win_size
        if rwnd is None:
            rwnd = constants.RCV_WIN_SIZE * mss

        if initial_seq < 0 or rwnd < 0:
            raise ValueError("initial_seq and rwnd must be non-negative")
        if mss <= 0 or max_segments <= 0 or rto <= 0 or duplicate_ack_threshold <= 0:
            raise ValueError("Buffer parameters must be positive")

        self.snd_base = initial_seq
        self.next_seq = initial_seq
        self.highest_sent = initial_seq

        self.MSS = mss
        self.mss = mss
        self.N = max_segments
        self.rwnd = rwnd
        self.rto = rto
        self.duplicate_ack_threshold = duplicate_ack_threshold
        self.dup_ack_count = 0

        self.send_buffer = {}
        self.timers = {}
        self._pending = []
        self.InFlight = 0
        self.out_of_order_tracker = {}

    def push_data(self, payload):
        """Enqueue payload bytes into segments of size MSS."""
        if not isinstance(payload, (bytes, bytearray)):
            raise TypeError("payload must be bytes")

        free_slots = self.N - len(self.send_buffer)
        if not payload or free_slots <= 0:
            return 0

        accepted = 0
        offset = 0

        while offset < len(payload) and free_slots > 0:
            chunk = bytes(payload[offset:offset + self.MSS])
            seq = self.next_seq

            self.send_buffer[seq] = {
                "payload": chunk,
                "length": len(chunk),
                "is_acked": False,
                "sent_time": None,
                "retries": 0,
            }
            self._pending.append(seq)

            chunk_len = len(chunk)
            self.next_seq += chunk_len
            accepted += chunk_len
            offset += chunk_len
            free_slots -= 1

        return accepted

    def has_unacked_data(self):
        return bool(self.send_buffer)

    @property
    def seq_to_send(self):
        return self._pending[0] if self._pending else self.highest_sent

    def can_send(self, cwnd):
        """Check if sending next segment is allowed by rwnd and cwnd."""
        if cwnd < 0:
            raise ValueError("cwnd must be non-negative")
        if not self._pending:
            return False

        seq = self._pending[0]
        size = len(self.send_buffer[seq]["payload"])

        cwnd_bytes = cwnd
        rwnd_bytes = self.rwnd

        return (self.InFlight + size <= rwnd_bytes and
                self.InFlight + size <= cwnd_bytes)

    def get_next_packet_to_send(self, now=None):
        if not self._pending:
            return None
        seq = self._pending[0]
        return seq, self.send_buffer[seq]["payload"]

    def mark_packet_sent(self, seq, now=None):
        """Mark segment sent and update timers and in-flight bytes."""
        current_time = time.monotonic() if now is None else now

        if self._pending and self._pending[0] == seq:
            self._pending.pop(0)
        elif seq not in self.send_buffer or self.send_buffer[seq].get("sent_time") is not None:
            return False

        entry = self.send_buffer.get(seq)
        if entry is None:
            return False

        entry["sent_time"] = current_time
        entry["retries"] = entry.get("retries", 0) + 1
        self.timers[seq] = current_time + self.rto
        self.InFlight += len(entry["payload"])
        self.highest_sent = max(self.highest_sent, seq + len(entry["payload"]))
        return True

    def on_ack_received(self, ack_seq, rcv_rwnd):
        """Process cumulative ACK; return (is_new_ack, loss_seq)."""
        if rcv_rwnd < 0:
            raise ValueError("rcv_rwnd must be non-negative")

        if ack_seq < self.snd_base or ack_seq > self.highest_sent:
            return False, None

        window_changed = (rcv_rwnd != self.rwnd)
        self.rwnd = rcv_rwnd

        if ack_seq == self.snd_base:
            if window_changed:
                return False, None
            has_inflight = any(s > ack_seq and e["sent_time"] is not None for s, e in self.send_buffer.items())
            if not has_inflight:
                return False, None

            count = self.out_of_order_tracker.get(ack_seq, 0) + 1
            self.out_of_order_tracker[ack_seq] = count
            self.dup_ack_count = count

            if count == self.duplicate_ack_threshold:
                return False, ack_seq
            return False, None

        acked_bytes = ack_seq - self.snd_base
        self.dup_ack_count = 0
        new_buf, new_timers, new_pending = {}, {}, []

        for seq, entry in sorted(self.send_buffer.items()):
            payload = entry["payload"]
            end_seq = seq + len(payload)

            if end_seq <= ack_seq:
                continue

            if seq < ack_seq < end_seq:
                trim = ack_seq - seq
                remaining = payload[trim:]
                new_seq = ack_seq

                updated = dict(entry)
                updated["payload"] = remaining
                updated["length"] = len(remaining)
                new_buf[new_seq] = updated

                if seq in self.timers:
                    new_timers[new_seq] = self.timers[seq]
                if seq in self._pending:
                    new_pending.append(new_seq)
                continue

            new_buf[seq] = entry
            if seq in self.timers:
                new_timers[seq] = self.timers[seq]
            if seq in self._pending:
                new_pending.append(seq)

        self.send_buffer = new_buf
        self.timers = new_timers
        self._pending = new_pending
        self.InFlight = max(0, self.InFlight - acked_bytes)
        self.snd_base = ack_seq

        self.out_of_order_tracker = {
            s: c for s, c in self.out_of_order_tracker.items() if s >= self.snd_base
        }

        return True, None

    def get_retransmission(self, seq, now=None):
        entry = self.send_buffer.get(seq)
        if entry is None or entry["sent_time"] is None:
            return None
        return seq, entry["payload"]

    def mark_retransmission_sent(self, seq, now=None):
        entry = self.send_buffer.get(seq)
        if entry is None or entry["sent_time"] is None:
            return False

        current_time = time.monotonic() if now is None else now
        entry["sent_time"] = current_time
        entry["retries"] = entry.get("retries", 0) + 1
        self.timers[seq] = current_time + self.rto
        return True

    def check_timeouts(self, now=None):
        current_time = time.monotonic() if now is None else now
        return [seq for seq, deadline in self.timers.items() if deadline <= current_time]

    def get_timed_out_packets(self, now=None):
        current_time = time.monotonic() if now is None else now
        timed_out = self.check_timeouts(current_time)
        return [(s, self.send_buffer[s]["payload"]) for s in timed_out if s in self.send_buffer]

    def get_inflight(self):
        return self.InFlight


class ReceiveBufferManager:
    """Manages receive window, cumulative ACK, and out-of-order reassembly."""

    def __init__(
        self,
        arg1=0,
        arg2=None,
        *,
        initial_seq=None,
        capacity=None,
        win_size=None,
        mss=constants.MSS,
    ):
        if initial_seq is not None:
            init_s = initial_seq
            cap = capacity if capacity is not None else (win_size * mss if win_size else constants.RCV_WIN_SIZE * mss)
        elif arg2 is not None:
            cap = arg1
            init_s = arg2
        else:
            init_s = arg1
            cap = capacity if capacity is not None else (win_size * mss if win_size else constants.RCV_WIN_SIZE * mss)

        if cap <= 0 or init_s < 0:
            raise ValueError("Invalid buffer capacity or initial_seq")

        self.lock = threading.RLock()
        self.capacity = cap
        self.mss = mss
        self.read_seq = init_s
        self.rcv_nxt = init_s
        self.rcv_buffer = {}
        self.fin_seq = None
        self.fin_received = False

    @property
    def rcv_base(self):
        return self.read_seq

    def get_rwnd(self):
        with self.lock:
            return max(0, self.capacity - len(self.rcv_buffer))

    def get_advertised_rwnd(self):
        with self.lock:
            if self.fin_received:
                return 0
            right_edge = self.read_seq + self.capacity
            window = max(0, right_edge - self.rcv_nxt)
            return min(window, 65535)

    def get_ack(self):
        with self.lock:
            return self.rcv_nxt % (2**32)

    def get_next_ack(self):
        return self.get_ack()

    def _unwrap(self, seq):
        seq_space = 2**32
        diff = (seq - self.rcv_nxt) % seq_space
        if diff >= seq_space // 2:
            diff -= seq_space
        return self.rcv_nxt + diff

    def discard(self):
        with self.lock:
            self.rcv_buffer.clear()
            self.read_seq = self.rcv_nxt
            self.fin_seq = None
            self.fin_received = False

    def _advance_ack(self):
        while self.rcv_nxt in self.rcv_buffer:
            if self.fin_seq is not None and self.rcv_nxt >= self.fin_seq:
                break
            self.rcv_nxt += 1

        if not self.fin_received and self.rcv_nxt == self.fin_seq:
            self.fin_received = True
            self.rcv_nxt += 1

    def on_fin_arrived(self, seq):
        if type(seq) is not int or seq < 0:
            raise ValueError("FIN seq must be a non-negative integer")
        with self.lock:
            seq = self._unwrap(seq)
            if self.fin_received or self.fin_seq is not None:
                return
            window_end = self.rcv_nxt + self.get_advertised_rwnd()
            if seq < self.rcv_nxt or seq >= window_end:
                return

            self.fin_seq = seq
            self.rcv_buffer = {p: v for p, v in self.rcv_buffer.items() if p < seq}
            self._advance_ack()

    def on_packet_arrived(self, seq, payload):
        with self.lock:
            if type(seq) is not int or seq < 0:
                raise ValueError("seq must be a non-negative integer")
            if not isinstance(payload, (bytes, bytearray)):
                raise TypeError("payload must be bytes")

            seq = self._unwrap(seq)
            if self.fin_received or not payload:
                return "DUPLICATE", self.get_rwnd()

            window_start = self.read_seq
            window_end = min(self.read_seq + self.capacity, self.rcv_nxt + 65535)
            packet_end = seq + len(payload)

            if packet_end <= window_start:
                return "DUPLICATE", self.get_rwnd()
            if seq >= window_end:
                return "OUT_OF_WINDOW", self.get_rwnd()

            start = max(seq, window_start)
            end = min(packet_end, window_end)
            if self.fin_seq is not None:
                end = min(end, self.fin_seq)

            new_bytes = {}
            for pos in range(start, end):
                if pos not in self.rcv_buffer:
                    new_bytes[pos] = payload[pos - seq]

            if not new_bytes:
                return "DUPLICATE", self.get_rwnd()
            if len(new_bytes) > self.get_rwnd():
                return "BUFFER_FULL", self.get_rwnd()

            self.rcv_buffer.update(new_bytes)
            self._advance_ack()
            return "ACCEPT", self.get_rwnd()

    def read_stream(self):
        with self.lock:
            data = bytearray()
            data_end = self.fin_seq if (self.fin_received and self.fin_seq is not None) else self.rcv_nxt
            while self.read_seq < data_end and self.read_seq in self.rcv_buffer:
                data.append(self.rcv_buffer.pop(self.read_seq))
                self.read_seq += 1
            return bytes(data)


class SenderFlowControl:
    """Thread-safe flow control window state manager."""

    def __init__(self, ack_seq, rwnd):
        if not 0 <= rwnd <= 65535:
            raise ValueError("rwnd must be between 0 and 65535 bytes")
        self._window_ack = ack_seq
        self._rwnd = rwnd
        self._version = 0
        self._closed = False
        self._changed = threading.Condition()

    @property
    def rwnd(self):
        with self._changed:
            return self._rwnd

    def update_window(self, ack_seq, rwnd, snd_base, next_seq):
        if not 0 <= rwnd <= 65535 or snd_base is None or next_seq is None:
            return False

        with self._changed:
            if self._closed or not (snd_base <= ack_seq <= next_seq) or ack_seq < self._window_ack:
                return False

            self._window_ack = ack_seq
            self._rwnd = rwnd
            self._version += 1
            self._changed.notify_all()
            return True

    def allowance(self, next_seq, requested):
        if requested < 0:
            raise ValueError("requested must not be negative")

        with self._changed:
            if self._closed:
                raise ConnectionError("connection is closed")
            available = max(0, self._window_ack + self._rwnd - next_seq)
            return min(requested, available), self._version

    def wait_for_update(self, version, timeout=None):
        with self._changed:
            changed = self._changed.wait_for(
                lambda: self._closed or self._version != version,
                timeout=timeout,
            )
            if self._closed:
                raise ConnectionError("connection is closed")
            return changed

    def close(self):
        with self._changed:
            self._closed = True
            self._version += 1
            self._changed.notify_all()
