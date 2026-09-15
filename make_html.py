import sys
import base64
import os
from markdown_it import MarkdownIt

img_path = r"C:\Users\Admin\.gemini\antigravity\brain\8a16b0a4-ad3e-4f77-8b90-e3db172491ca\.user_uploaded\media_1789025278612.png"
with open(img_path, "rb") as f:
    img_b64 = base64.b64encode(f.read()).decode("utf-8")

md_file = r"D:\ViettelCloudCamera\cloud-camera-microservice-v2\cloud-camera\ARCHITECTURE-AI.md"
with open(md_file, "r", encoding="utf-8") as f:
    content = f.read()

# Thay thế khối ```mermaid ... ``` bằng ảnh sơ đồ kiến trúc
import re
diagram_html = f'<div style="text-align: center; margin: 24px 0;"><img src="data:image/png;base64,{img_b64}" style="max-width: 95%; height: auto; border: 1px solid #ddd; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1);" /><p style="font-style: italic; color: #666; margin-top: 8px; font-size: 13px;">Hình 1: Sơ đồ kiến trúc tổng thể luồng xử lý AI Cloud Camera</p></div>'
content_replaced = re.sub(r'```mermaid[\s\S]*?```', diagram_html, content)

md = MarkdownIt()
html_body = md.render(content_replaced)

full_html = f"""<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<title>KIẾN TRÚC HỆ THỐNG XỬ LÝ AI CLOUD CAMERA</title>
<style>
    @page {{
        size: A4;
        margin: 18mm 16mm 18mm 16mm;
    }}
    body {{
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
        line-height: 1.6;
        color: #1f2937;
        font-size: 13.5px;
        background: #fff;
    }}
    h1 {{
        color: #1e3a8a;
        font-size: 22px;
        border-bottom: 2px solid #3b82f6;
        padding-bottom: 8px;
        margin-top: 0;
    }}
    h2 {{
        color: #1e40af;
        font-size: 17px;
        border-bottom: 1px solid #e5e7eb;
        padding-bottom: 6px;
        margin-top: 24px;
    }}
    h3 {{
        color: #374151;
        font-size: 15px;
        margin-top: 18px;
    }}
    h4 {{
        color: #4b5563;
        font-size: 14px;
        margin-top: 14px;
    }}
    p, li {{
        text-align: justify;
    }}
    code {{
        background-color: #f3f4f6;
        color: #b91c1c;
        padding: 2px 5px;
        border-radius: 4px;
        font-family: Consolas, monospace;
        font-size: 12.5px;
    }}
    pre {{
        background: #1e293b;
        color: #f8fafc;
        padding: 12px;
        border-radius: 6px;
        overflow-x: auto;
        font-size: 12px;
    }}
    pre code {{
        background: none;
        color: inherit;
        padding: 0;
    }}
    table {{
        width: 100%;
        border-collapse: collapse;
        margin: 16px 0;
        font-size: 13px;
    }}
    th, td {{
        border: 1px solid #d1d5db;
        padding: 8px 12px;
        text-align: left;
    }}
    th {{
        background-color: #f8fafc;
        color: #1e293b;
        font-weight: 600;
    }}
    tr:nth-child(even) {{
        background-color: #f9fafb;
    }}
    blockquote {{
        border-left: 4px solid #3b82f6;
        padding: 8px 16px;
        margin: 16px 0;
        background-color: #eff6ff;
        color: #1e40af;
    }}
</style>
</head>
<body>
{html_body}
</body>
</html>
"""

html_file = r"D:\ViettelCloudCamera\cloud-camera-microservice-v2\cloud-camera\temp_arch_doc.html"
with open(html_file, "w", encoding="utf-8") as f:
    f.write(full_html)
print("Da tao xong HTML:", html_file)
