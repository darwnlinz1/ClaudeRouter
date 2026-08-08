# MASTER PROMPT — NÂNG CẤP TOÀN DIỆN LOCAL MULTI-AGENT AI ORCHESTRATOR

Bạn là Principal Engineer chịu trách nhiệm nâng cấp một Local Multi-Agent AI Orchestrator gồm FastAPI, Python orchestration core, Claude cookie routing, Git safety layer và giao diện HTML/Tailwind/JavaScript dùng SSE.

## Mục tiêu

Biến hệ thống thành một Auto-Coder local an toàn, có thể quan sát, tự phục hồi và dễ bảo trì mà không thay đổi nguyên tắc: không tự push mã nguồn; Supervisor chỉ lập kế hoạch; Worker chỉ sửa đúng file được giao; Reviewer/Tester chỉ nghiệm thu; Python backend là nguồn sự thật cuối cùng.

## Yêu cầu bắt buộc

1. Mọi đường dẫn do User hoặc Agent cung cấp phải được chuẩn hóa, resolve và chứng minh nằm dưới project root trước khi đọc hoặc ghi.
2. Supervisor không được mở rộng quyền sửa file bằng `delegate_task`. Worker chỉ được sửa file đã có trong context hoặc file mới được cấp quyền rõ ràng.
3. Worker phải trả `worker_feedback`; backend lưu đồng thời feedback và kết quả Patch/Syntax/Test; Supervisor phải đối chiếu hai nguồn ở lượt tiếp theo.
4. Patch Engine hỗ trợ SEARCH/REPLACE nguyên tử, anchor duy nhất và tạo file mới chỉ khi SEARCH rỗng và target được cấp quyền.
5. Sau khi machine gate Pass, Reviewer/Tester độc lập phải duyệt patch. Nếu Reviewer yêu cầu revise, backend rollback file và chuyển feedback về Supervisor ở lượt kế tiếp.
6. LLM retry phải có giới hạn, backoff, tự chuyển sang tài khoản Worker/Reviewer khác và trạng thái lỗi rõ ràng; không được treo vô hạn.
7. `state.json` phải ghi nguyên tử; lịch sử lỗi và feedback phải có giới hạn kích thước.
8. Git snapshot chỉ hoạt động local. Không được push. Không được làm mất thay đổi ngoài phạm vi file Worker đang sửa.
9. Backend phải phát SSE có cấu trúc cho vòng đời task, agent, patch, test, review, feedback và kết quả cuối.
10. UI phải đóng SSE đúng lúc, hiển thị Pass/Fail, review verdict, feedback, turn history, trạng thái kết nối và lỗi mạng.
11. Không đưa dữ liệu AI/User trực tiếp vào `innerHTML`. Markdown phải được sanitize; dữ liệu log/todo/diff phải escape.
12. UI phải cập nhật token/account counters, hỗ trợ keyboard/accessibility, responsive layout và nói rõ giới hạn drag-and-drop của trình duyệt.
13. Dependency manifest phải đầy đủ; test phải phản ánh đúng kiến trúc Supervisor → Worker → Reviewer.
14. UI và backend phải cho phép chọn `model`/`effort` độc lập cho Supervisor, Worker và Reviewer; mỗi SSE status phải cho biết vai trò nào đang dùng cấu hình nào.
15. Chat mode phải hoạt động độc lập không cần project root, đồng thời ẩn toàn bộ cấu hình và panel chỉ dành cho Orchestrator.
16. Dashboard phải lưu task bền vững ngoài target repository, hiển thị danh sách dọc, phase/status, cấu hình riêng và thống kê file đã được duyệt theo số dòng thêm/xóa.
17. New Project phải chạy trong artifact staging ngoài destination, hỗ trợ nhiều file bằng `is_final_ticket`, chỉ auto-apply sau machine Pass + Reviewer approved, và tạo ZIP tải về khi được cấu hình.

## Luồng lý tưởng

1. User chọn project root, source files, model, routing và test command.
2. Backend xác thực root/file, tạo task ID không đoán được và khởi tạo stream.
3. Supervisor đọc task, rules, decisions, state và source context đã giới hạn kích thước.
4. Supervisor gọi `request_context` hoặc `delegate_task`.
5. Backend kiểm tra capability của target trước khi gọi Worker.
6. Worker nhận đúng một file, trả `<patch>` và `worker_feedback`.
7. Backend snapshot local, áp dụng patch, chạy syntax/test và rollback khi lỗi.
8. Nếu machine gate Pass, Reviewer kiểm tra đúng yêu cầu, phạm vi, edge case và bảo mật.
9. Backend lưu feedback của Worker, kết quả máy, verdict/feedback của Reviewer và phát event SSE có cấu trúc.
10. Nếu Reviewer yêu cầu revise, backend rollback target; lượt Supervisor tiếp theo đối chiếu cả ba nguồn để giao lại việc.
11. Khi machine gate Pass và Reviewer approved, backend phát summary; UI đóng stream và hiển thị toàn bộ kết quả.

## Tiêu chí hoàn thành

- Không còn read/write path traversal.
- Không retry vô hạn.
- Không render HTML chưa sanitize.
- Tất cả task đều kết thúc ở trạng thái rõ ràng.
- Feedback và execution result hiển thị trong UI.
- Test hiện có và test mới đều pass.
- Không commit hoặc push tự động khi thực hiện prompt nâng cấp này.
