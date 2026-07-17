"""WebDAV 辅助函数：路径解析、PROPFIND 响应、目录 HTML。"""

import xml.etree.ElementTree as ET
from typing import Tuple, Optional, List
from datetime import datetime, timezone
from html import escape as html_escape
from core.base import FileItem

import mimetypes

NS = {
    'D': 'DAV:',
    'xmlns:D': 'DAV:'
}

WEBDAV_TIME_FORMAT = '%a, %d %b %Y %H:%M:%S GMT'
WEBDAV_ISO_TIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_webdav_path(path: str) -> Tuple[Optional[str], str]:
    """/dav/{account}/{file_path} -> (account, file_path)；根目录返回 (None, '/')。"""
    if not path or path == "":
        return None, "/"

    path_parts = [p for p in path.strip('/').split('/') if p]

    if not path_parts:
        return None, "/"

    if path_parts[0] == "dav":
        if len(path_parts) == 1:
            return None, "/"
        elif len(path_parts) == 2:
            return path_parts[1], "/"
        else:
            account_name = path_parts[1]
            file_path = "/".join(path_parts[2:])
            return account_name, file_path
    else:
        # 兼容没有 /dav 前缀的历史路径
        account_name = path_parts[0]
        if len(path_parts) == 1:
            file_path = "/"
        else:
            file_path = "/".join(path_parts[1:])
        return account_name, file_path


def generate_propfind_response(file_info: FileItem, path: str) -> str:
    root = ET.Element('{DAV:}multistatus')

    response = ET.SubElement(root, '{DAV:}response')

    href = ET.SubElement(response, '{DAV:}href')
    href.text = path

    propstat = ET.SubElement(response, '{DAV:}propstat')

    prop = ET.SubElement(propstat, '{DAV:}prop')

    resourcetype = ET.SubElement(prop, '{DAV:}resourcetype')
    if file_info.is_dir:
        ET.SubElement(resourcetype, '{DAV:}collection')

    if not file_info.is_dir:
        getcontentlength = ET.SubElement(prop, '{DAV:}getcontentlength')
        getcontentlength.text = str(file_info.size)

        getlastmodified = ET.SubElement(prop, '{DAV:}getlastmodified')
        getlastmodified.text = ensure_utc(file_info.modified).strftime(WEBDAV_TIME_FORMAT) if file_info.modified else datetime.now(timezone.utc).strftime(WEBDAV_TIME_FORMAT)

        creationdate = ET.SubElement(prop, '{DAV:}creationdate')
        creationdate.text = ensure_utc(file_info.created).strftime(WEBDAV_ISO_TIME_FORMAT) if file_info.created else datetime.now(timezone.utc).strftime(WEBDAV_ISO_TIME_FORMAT)

    getcontenttype = ET.SubElement(prop, '{DAV:}getcontenttype')
    if file_info.is_dir:
        getcontenttype.text = 'httpd/unix-directory'
    else:
        import mimetypes
        mime_type, _ = mimetypes.guess_type(file_info.name)
        getcontenttype.text = mime_type or 'application/octet-stream'

    status = ET.SubElement(propstat, '{DAV:}status')
    status.text = 'HTTP/1.1 200 OK'

    return ET.tostring(root, encoding='unicode', xml_declaration=True)


def generate_directory_html(files: List[FileItem], webdav_path: str) -> str:
    # 刻意走内联 style，避免浏览器以外的 WebDAV 客户端解析外部 CSS 出问题
    safe_path = html_escape(webdav_path)
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Directory listing for {safe_path}</title>
    <meta charset="utf-8">
</head>
<body style="font-family: Arial, sans-serif; margin: 20px;">
    <h1>Directory listing for {safe_path}</h1>
    <table style="border-collapse: collapse; width: 100%; border: 1px solid #ddd;">
        <tr style="background-color: #f2f2f2;">
            <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Name</th>
            <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Size</th>
            <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Modified</th>
        </tr>"""
    
    if webdav_path != "/":
        parent_path = "/".join(webdav_path.split("/")[:-1]) or "/"
        html += f"""
        <tr>
            <td style="border: 1px solid #ddd; padding: 8px;"><a href="{parent_path}" style="color: #0066cc;">../</a></td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">-</td>
            <td style="border: 1px solid #ddd; padding: 8px;">-</td>
        </tr>"""

    for file in files:
        file_icon = "📁" if file.is_dir else "📄"
        file_size = "-" if file.is_dir else f"{file.size:,}"
        modified_time = file.modified.strftime('%Y-%m-%d %H:%M:%S') if file.modified else "-"
        link_color = "#0066cc" if file.is_dir else "#333"
        safe_name = html_escape(file.name)
        
        html += f"""
        <tr>
            <td style="border: 1px solid #ddd; padding: 8px;"><a href="{webdav_path.rstrip('/')}/{safe_name}" style="color: {link_color};">{file_icon} {safe_name}</a></td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">{file_size}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{modified_time}</td>
        </tr>"""
    
    html += """
    </table>
</body>
</html>"""
    
    return html


def get_mime_type(filename: str) -> str:
    import mimetypes
    mime_type, _ = mimetypes.guess_type(filename)
    return mime_type or 'application/octet-stream'


def format_file_size(size: int) -> str:
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    if size <= 0:
        return '0 B'
    value = float(size)
    unit_index = 0
    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024
        unit_index += 1
    decimals = 0 if unit_index == 0 else 2
    return f"{value:.{decimals}f} {units[unit_index]}" 