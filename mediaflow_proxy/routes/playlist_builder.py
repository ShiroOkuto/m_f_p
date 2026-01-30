import json
import logging
import urllib.parse
from typing import Iterator, Dict, Optional
from fastapi import APIRouter, Request, HTTPException, Query
from fastapi.responses import StreamingResponse
from starlette.responses import RedirectResponse

from mediaflow_proxy.configs import settings
from mediaflow_proxy.utils.http_utils import get_original_scheme
from mediaflow_proxy.utils.http_client import create_aiohttp_session
import asyncio

logger = logging.getLogger(__name__)
playlist_builder_router = APIRouter()


def rewrite_m3u_links_streaming(
    m3u_lines_iterator: Iterator[str], base_url: str, api_password: Optional[str]
) -> Iterator[str]:
    """
    Riscrive i link da un iteratore di linee M3U raggruppando per entry (#EXTINF).
    Gestisce correttamente gli header e le proprietà KODI anche se appaiono dopo l'URL.
    """
    current_entry_lines = []

    def process_entry(entry_lines):
        if not entry_lines:
            return []

        headers = {}
        kodi_props = {}
        url_idx = -1
        processed_lines = []

        # Primo passaggio: raccogli info e trova l'URL
        for idx, line in enumerate(entry_lines):
            logical_line = line.strip()
            if not logical_line:
                continue

            if logical_line.startswith("#EXTINF:"):
                processed_lines.append(line)
            elif logical_line.startswith("#EXTVLCOPT:"):
                processed_lines.append(line)
                try:
                    option_str = logical_line.split(":", 1)[1]
                    if "=" in option_str:
                        key_vlc, value_vlc = option_str.split("=", 1)
                        key_vlc, value_vlc = key_vlc.strip(), value_vlc.strip()
                        if key_vlc == "http-header" and ":" in value_vlc:
                            h_key, h_val = value_vlc.split(":", 1)
                            headers[h_key.strip()] = h_val.strip()
                        elif key_vlc.startswith("http-"):
                            headers[key_vlc[len("http-") :]] = value_vlc
                except Exception:
                    pass
            elif logical_line.startswith("#EXTHTTP:"):
                processed_lines.append(line)
                try:
                    json_str = logical_line.split(":", 1)[1]
                    headers.update(json.loads(json_str))
                except Exception:
                    pass
            elif logical_line.startswith("#KODIPROP:"):
                processed_lines.append(line)
                try:
                    prop_str = logical_line.split(":", 1)[1]
                    if "=" in prop_str:
                        k_key, k_val = prop_str.split("=", 1)
                        kodi_props[k_key.strip()] = k_val.strip()
                except Exception:
                    pass
            elif logical_line.startswith("http"):
                url_idx = len(processed_lines)
                processed_lines.append(line)
            elif logical_line.startswith("#"):
                processed_lines.append(line)

        if url_idx == -1:
            return entry_lines

        # Secondo passaggio: riscrivi l'URL
        original_url = processed_lines[url_idx].strip()
        manifest_type = kodi_props.get("inputstream.adaptive.manifest_type", "").lower()
        
        processed_url = original_url
        if "pluto.tv" in original_url:
            pass
        elif "vavoo.to" in original_url:
            encoded = urllib.parse.quote(original_url, safe="")
            processed_url = f"{base_url}/proxy/hls/manifest.m3u8?d={encoded}"
        elif "vixsrc.to" in original_url:
            encoded = urllib.parse.quote(original_url, safe="")
            processed_url = f"{base_url}/extractor/video?host=VixCloud&redirect_stream=true&d={encoded}&max_res=true&no_proxy=true"
        elif manifest_type == "mpd" or ".mpd" in original_url:
            from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
            parsed = urlparse(original_url)
            query = parse_qs(parsed.query)
            key_id = query.get("key_id", [None])[0]
            key = query.get("key", [None])[0]
            
            clean_q = urlencode({k: v for k, v in query.items() if k not in ["key_id", "key"]}, doseq=True)
            clean_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, clean_q, ""))
            
            processed_url = f"{base_url}/proxy/mpd/manifest.m3u8?d={urllib.parse.quote(clean_url, safe='')}"
            if key_id: processed_url += f"&key_id={key_id}"
            if key: processed_url += f"&key={key}"
        else:
            encoded = urllib.parse.quote(original_url, safe="")
            processed_url = f"{base_url}/proxy/hls/manifest.m3u8?d={encoded}"

        # Aggiungi chiavi KODI
        license_key = kodi_props.get("inputstream.adaptive.license_key")
        if license_key and ":" in license_key:
            kid, k = license_key.split(":", 1)
            if "&key_id=" not in processed_url: processed_url += f"&key_id={kid}"
            if "&key=" not in processed_url: processed_url += f"&key={k}"

        # Aggiungi Headers
        if headers:
            h_str = "".join([f"&h_{urllib.parse.quote(k)}={urllib.parse.quote(v)}" for k, v in headers.items()])
            processed_url += h_str

        if api_password:
            processed_url += f"&api_password={api_password}"

        processed_lines[url_idx] = processed_url + "\n"
        return processed_lines

    for line in m3u_lines_iterator:
        if line.strip().startswith("#EXTINF:"):
            for p_line in process_entry(current_entry_lines):
                yield p_line
            current_entry_lines = [line]
        elif line.strip().startswith("#EXTM3U"):
            yield line
        else:
            current_entry_lines.append(line)

    for p_line in process_entry(current_entry_lines):
        yield p_line


async def async_download_m3u_playlist(url: str) -> list[str]:
    """Scarica una playlist M3U in modo asincrono e restituisce le righe."""
    headers = {
        "User-Agent": settings.user_agent,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }
    lines = []
    try:
        async with create_aiohttp_session(url, timeout=30) as (session, proxy_url):
            response = await session.get(url, headers=headers, proxy=proxy_url)
            response.raise_for_status()
            content = await response.text()
            # Split content into lines
            for line in content.splitlines():
                lines.append(line + "\n" if line else "")
    except Exception as e:
        logger.error(f"Error downloading playlist (async): {str(e)}")
        raise
    return lines


def parse_channel_entries(lines: list[str]) -> list[list[str]]:
    """
    Analizza le linee di una playlist M3U e le raggruppa in entry di canali.
    Ogni entry contiene tutto ciò che si trova tra un #EXTINF e il successivo.
    """
    entries = []
    current_entry = []
    for line in lines:
        if line.strip().startswith("#EXTINF:"):
            if current_entry:
                entries.append(current_entry)
            current_entry = [line]
        elif current_entry:
            current_entry.append(line)
    if current_entry:
        entries.append(current_entry)
    return entries


async def async_generate_combined_playlist(playlist_definitions: list[str], base_url: str, api_password: Optional[str]):
    """Genera una playlist combinata da multiple definizioni, scaricando in parallelo."""
    # Prepara i task di download
    download_tasks = []
    for definition in playlist_definitions:
        should_proxy = True
        playlist_url_str = definition
        should_sort = False

        if definition.startswith("sort:"):
            should_sort = True
            definition = definition[len("sort:") :]

        if definition.startswith("no_proxy:"):  # Può essere combinato con sort:
            should_proxy = False
            playlist_url_str = definition[len("no_proxy:") :]
        else:
            playlist_url_str = definition

        download_tasks.append({"url": playlist_url_str, "proxy": should_proxy, "sort": should_sort})

    # Scarica tutte le playlist in parallelo
    results = await asyncio.gather(
        *[async_download_m3u_playlist(task["url"]) for task in download_tasks], return_exceptions=True
    )

    # Raggruppa le playlist da ordinare e quelle da non ordinare
    sorted_playlist_lines = []
    unsorted_playlists_data = []

    for idx, result in enumerate(results):
        task_info = download_tasks[idx]
        if isinstance(result, Exception):
            # Aggiungi errore come playlist non ordinata
            unsorted_playlists_data.append(
                {"lines": [f"# ERROR processing playlist {task_info['url']}: {str(result)}\n"], "proxy": False}
            )
            continue

        if task_info.get("sort", False):
            sorted_playlist_lines.extend(result)
        else:
            unsorted_playlists_data.append({"lines": result, "proxy": task_info["proxy"]})

    # Gestione dell'header #EXTM3U
    first_playlist_header_handled = False

    def yield_header_once(lines_iter):
        nonlocal first_playlist_header_handled
        has_header = False
        for line in lines_iter:
            is_extm3u = line.strip().startswith("#EXTM3U")
            if is_extm3u:
                has_header = True
                if not first_playlist_header_handled:
                    first_playlist_header_handled = True
                    yield line
            else:
                yield line
        if has_header and not first_playlist_header_handled:
            first_playlist_header_handled = True

    # 1. Processa e ordina le playlist marcate con 'sort'
    if sorted_playlist_lines:
        # Estrai le entry dei canali
        # Modifica: Estrai le entry e mantieni l'informazione sul proxy
        channel_entries_with_proxy_info = []
        for idx, result in enumerate(results):
            task_info = download_tasks[idx]
            if task_info.get("sort") and isinstance(result, list):
                entries = parse_channel_entries(result)  # result è la lista di linee della playlist
                for entry_lines in entries:
                    # L'opzione proxy si applica a tutto il blocco del canale
                    channel_entries_with_proxy_info.append((entry_lines, task_info["proxy"]))

        # Ordina le entry in base al nome del canale (da #EXTINF)
        # La prima riga di ogni entry è sempre #EXTINF
        channel_entries_with_proxy_info.sort(key=lambda x: x[0][0].split(",")[-1].strip())

        # Gestisci l'header una sola volta per il blocco ordinato
        if not first_playlist_header_handled:
            yield "#EXTM3U\n"
            first_playlist_header_handled = True

        # Applica la riscrittura dei link in modo selettivo
        for entry_lines, should_proxy in channel_entries_with_proxy_info:
            # L'URL è l'ultima riga dell'entry
            url = entry_lines[-1]
            # Yield tutte le righe prima dell'URL
            for line in entry_lines[:-1]:
                yield line

            if should_proxy:
                # Usa un iteratore fittizio per processare una sola linea
                rewritten_url_iter = rewrite_m3u_links_streaming(iter([url]), base_url, api_password)
                yield next(rewritten_url_iter, url)  # Prende l'URL riscritto, con fallback all'originale
            else:
                yield url  # Lascia l'URL invariato

    # 2. Accoda le playlist non ordinate
    for playlist_data in unsorted_playlists_data:
        lines_iterator = iter(playlist_data["lines"])
        if playlist_data["proxy"]:
            lines_iterator = rewrite_m3u_links_streaming(lines_iterator, base_url, api_password)

        for line in yield_header_once(lines_iterator):
            yield line


@playlist_builder_router.get("/playlist")
async def proxy_handler(
    request: Request,
    d: str = Query(..., description="Query string con le definizioni delle playlist", alias="d"),
    api_password: Optional[str] = Query(None, description="Password API per MFP"),
):
    """
    Endpoint per il proxy delle playlist M3U con supporto MFP.

    Formato query string: playlist1&url1;playlist2&url2
    Esempio: https://mfp.com:pass123&http://provider.com/playlist.m3u
    """
    try:
        if not d:
            raise HTTPException(status_code=400, detail="Query string mancante")

        if not d.strip():
            raise HTTPException(status_code=400, detail="Query string cannot be empty")

        # Validate that we have at least one valid definition
        playlist_definitions = [def_.strip() for def_ in d.split(";") if def_.strip()]
        if not playlist_definitions:
            raise HTTPException(status_code=400, detail="No valid playlist definitions found")

        # Costruisci base_url con lo schema corretto
        original_scheme = get_original_scheme(request)
        base_url = f"{original_scheme}://{request.url.netloc}"

        # Estrai base_url dalla prima definizione se presente
        if playlist_definitions and "&" in playlist_definitions[0]:
            parts = playlist_definitions[0].split("&", 1)
            if ":" in parts[0] and not parts[0].startswith("http"):
                # Estrai base_url dalla prima parte se contiene password
                base_url_part = parts[0].rsplit(":", 1)[0]
                if base_url_part.startswith("http"):
                    base_url = base_url_part

        async def generate_response():
            async for line in async_generate_combined_playlist(playlist_definitions, base_url, api_password):
                yield line

        return StreamingResponse(
            generate_response(),
            media_type="application/vnd.apple.mpegurl",
            headers={"Content-Disposition": 'attachment; filename="playlist.m3u"', "Access-Control-Allow-Origin": "*"},
        )

    except Exception as e:
        logger.error(f"General error in playlist handler: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}") from e


@playlist_builder_router.get("/builder")
async def url_builder():
    """
    Pagina con un'interfaccia per generare l'URL del proxy MFP.
    """
    return RedirectResponse(url="/playlist_builder.html")
