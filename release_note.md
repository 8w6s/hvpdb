# 🚀 HVPDB v1.0.3 Update Roadmap: "The Polishing Phase"
*Gửi người anh em thiện lành,*

Sorry vì đã "cầm đèn chạy trước ô tô"! Đã điều chỉnh lại lộ trình, chúng ta sẽ dồn toàn lực để hoàn thiện phiên bản **v1.0.3** này trở nên hoàn hảo nhất trước khi nghĩ đến những thứ xa xôi.

Dưới đây là kế hoạch chi tiết cho **v1.0.3 Update**:

---

## 1. 🐛 Bug Fix: "The Vanishing Prompt" (Ưu tiên P0)
**Vấn đề:** Khi backspace hết ký tự, prompt `hvpdb >` biến mất.
- **Nguyên nhân:** `rich.console.input` in prompt như text thường, không phải là system prompt cứng. Trên Windows Console, con trỏ có thể lùi về quá khứ!
- **Giải pháp:**
  - Chuyển sang dùng thư viện `prompt_toolkit` (nếu được phép thêm dependency) HOẶC tự handle buffer.
  - **Quyết định:** Thử nghiệm fix bằng cách ép kiểu con trỏ hoặc dùng `cmd` loop chuẩn của Python nhưng override `stdout`.

## 2. 🔒 Security: "Anti-Copy" (Machine Binding)
**Vấn đề:** Chống copy file database sang máy khác.
- **Giải pháp cho v1.0.3:**
  - Thêm một file `machine.id` ẩn hoặc check `uuid.getnode()` khi `init`.
  - Khi `connect`, so khớp ID này. Nếu sai -> Cảnh báo hoặc chặn (tùy mức độ gắt).
  - *Lưu ý:* Đây là biện pháp "soft lock" cho v1.0.3.

## 3. 🔑 Auth: "Access Key" (QR & Keyfiles)
**Thay đổi:** Đổi tên từ "Passkey" -> "Access Key" để tránh nhầm lẫn với FIDO/WebAuthn.
**Vấn đề:** Đăng nhập tiện lợi hơn mà không cần nhớ password.
- **Triển khai v1.0.3:**
  - Thêm tùy chọn `--access-key` cho CLI.
  - Lệnh `hvpdb gen-key` để tạo file key ngẫu nhiên.
  - QR Code: In Access Key ra terminal để user lưu vào mobile (dạng text an toàn).

## 4. ⚡ Storage Optimization: "Anti-Bloat Strategy"
**Cảm nhận của tôi:** Hiện tại WAL (Write-Ahead Log) là cơ chế append-only. Nếu update liên tục, file log sẽ phình to khủng khiếp dù dữ liệu thực tế bé xíu.
**Giải pháp cho v1.0.3:**
  1. **Aggressive Auto-Checkpoint:** Kích hoạt `checkpoint` (merge log vào data chính) tự động khi tỷ lệ Log/Data vượt quá ngưỡng (ví dụ: Log > 50% Data).
  2. **Zstd Dictionary Training (Future):** Với v1.0.3, ta chỉ cần tăng level nén. Nhưng tương lai, nên "học" cấu trúc dữ liệu để nén các key lặp lại (như `_id`, `created_at`).
  3. **Delta Encoding (Future):** Thay vì ghi đè cả document, chỉ ghi phần *thay đổi* vào WAL.

---

## 📝 Next Action
Tôi đang tiến hành tích hợp **`prompt_toolkit`** vào `hvpshell.py` ngay lập tức để xử lý vụ "Vanishing Prompt". Cảm giác gõ lệnh sắp "sướng" hơn nhiều rồi đấy!

