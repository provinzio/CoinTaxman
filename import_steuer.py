#!/usr/bin/env python3
"""
Import Krypto-Veräußerungsgeschäfte in SteuerSparErklärung 2025 via pywinauto.

Verwendung:
  python import_steuer.py --inspect        # Zeigt UI-Elemente (Toolbar/Nav/Form) an
    python import_steuer.py --calibrate      # Kalibriert Feldkoordinaten per Mausposition
  python import_steuer.py --dry-run        # Simuliert Import ohne Eingabe
  python import_steuer.py                  # Führt Import durch
  python import_steuer.py --start-at 5    # Startet ab Zeile 5 (1-basiert)

Voraussetzungen:
  pip install pywinauto
  SteuerSparErklärung 2025 muss geöffnet sein und auf
  "Private Veräußerungsgeschäfte" navigiert sein.
"""

import csv
import json
import re
import sys
import time
import argparse
import ctypes
import subprocess
import threading
from pathlib import Path

# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------

CSV_PATH = Path(__file__).parent / "export" / "veräußerung crypto.csv"
COORDS_PATH = Path(__file__).parent / "data" / "steuer_coords.json"

# Fenstertitel-Teilstring (Groß/Kleinschreibung irrelevant)
APP_TITLE_PATTERN = "SteuerSpar"

# Bekannte auto_ids (aus --inspect / --inspect-nav bestätigt)
_AUTO_ID_TOOLBAR = "SSE_Application.AAV4GLEngineWindow31.MainToolBar"
_AUTO_ID_NAV = "SSE_Application.AAV4GLEngineWindow31.centralWidget.SearchSplitter.TopLevelHSplitter.NavFrameSSE"
_AUTO_ID_NAV_TREE = "SSE_Application.AAV4GLEngineWindow31.centralWidget.SearchSplitter.TopLevelHSplitter.NavFrameSSE.QWidget.NavWidgetSSE"
_AUTO_ID_FORM = "SSE_Application.AAV4GLEngineWindow31.centralWidget.SearchSplitter.TopLevelHSplitter.RedThreadContent"
_AUTO_ID_FORM_HDR = "SSE_Application.AAV4GLEngineWindow31.centralWidget.SearchSplitter.TopLevelHSplitter.RedThreadContent.ClientFrameSSE.ClientHeader.QLabel"
_AUTO_ID_BTN_ANLAGE = "SSE_Application.AAV4GLEngineWindow31.MainToolBar.tb_anlage"

# Bevorzugter In-Form-Button zum Anlegen eines weiteren Eintrags
_BTN_TEXT_WEITERES = "Weiteres Veräußerungsgeschäft erfassen"
_BTN_TEXT_WEITERES_RE = r"Weiteres Ver.*u.*erungsgesch.*ft erfassen"
_VERAEUSSERUNGSOBJEKT_VALUE = "Kryptowerte"
_WERBUNGSKOSTEN_ART_VALUE = "Gebühr"

# Exakter Nav-Knoten-Titel (aus --inspect-nav bestätigt)
_NAV_TITLE_VERAUESSERUNG = "Private Veräußerungsgeschäfte"

# Wartezeiten (Sekunden) – bei langsamen Rechnern erhöhen
DELAY_FIELD = 0.10   # zwischen Tasteneingaben
DELAY_ENTRY = 1.50   # nach "Neuen Eintrag anlegen" (Formular laden)
DELAY_NAV = 2.00   # nach Nav-Klick (Formular muss umschalten)
DELAY_CLICK = 0.30   # nach sonstigem Mausklick

_COORD_KEYS = [
    "dropdown",
    "bezeichnung",
    "verkauf_am",
    "verkauf_preis",
    "kauf_am",
    "kauf_preis",
    "werbungskosten_art",
    "werbungskosten",
]

_OPTIONAL_COORD_KEYS = [
    "footer_weiter",
    "weiteres_veraeusserungsgeschaeft",
]

_UI_STATE = {
    "in_werbungskosten_submenu": False,
}

# Der In-Form-Button kann sich vertikal verschieben, wenn viele Einträge vorhanden sind.
# Deshalb nicht primär über fixe Koordinate klicken.
_PREFER_DYNAMIC_WEITERES_BUTTON = True
_USE_WEITERES_COORD_FALLBACK = False

# ---------------------------------------------------------------------------
# CSV laden
# ---------------------------------------------------------------------------


_CSV_HEADER_ALIASES = {
    "Bezeichnung": ["Bezeichnung", "Währung"],
    "Kauf_am": ["Kauf_am", "Erwerbsdatum"],
    "Verkauf_am": ["Verkauf_am", "Verkaufsdatum"],
    "Verkaufspreis": ["Verkaufspreis", "Veräußerungserlös in EUR"],
    "Kaufpreis": ["Kaufpreis", "Anschaffungskosten in EUR"],
    "Werbungskosten": ["Werbungskosten", "Werbungskosten in EUR"],
}


def _resolve_header(row: dict, canonical_name: str) -> str:
    """Liest einen Feldwert über bekannte Header-Aliasse aus."""
    for candidate in _CSV_HEADER_ALIASES[canonical_name]:
        if candidate in row and row[candidate] is not None:
            return row[candidate]
    return ""


def _detect_missing_headers(fieldnames: list[str]) -> list[str]:
    """Ermittelt kanonische Felder, für die kein unterstützter Header vorhanden ist."""
    available = set(fieldnames or [])
    missing = []
    for canonical_name, aliases in _CSV_HEADER_ALIASES.items():
        if not any(alias in available for alias in aliases):
            missing.append(canonical_name)
    return missing


def _normalize_date(value: str) -> str:
    """Extrahiert DD.MM.YYYY aus Datum + optionaler Uhrzeit."""
    return (value or "").split()[0]


def _normalize_amount(value: str, decimals: int | None = None) -> str:
    """Normalisiert Zahlen auf deutsches Kommaformat; optional auf N Nachkommastellen."""
    text = (value or "").strip()
    if not text:
        return "0,00" if decimals is not None else "0"
    if decimals is None:
        return text

    amount = float(text.replace(",", "."))
    return f"{amount:.{decimals}f}".replace(".", ",")


def _is_zero(value: str) -> bool:
    """Prüft ob ein normalisierter Betrag effektiv 0 ist."""
    try:
        return float(value.replace(",", ".")) == 0.0
    except (ValueError, AttributeError):
        return False


def load_transactions(csv_path: Path) -> list[dict]:
    """Liest Transaktionen; unterstützt alte und neue CSV-Header."""
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        missing = _detect_missing_headers(reader.fieldnames or [])
        if missing:
            readable_missing = ", ".join(missing)
            raise ValueError(
                "CSV-Header nicht unterstützt. Fehlende Felder: "
                f"{readable_missing}"
            )

        rows = []
        skipped = 0
        for raw_row in reader:
            row = {
                "Bezeichnung": _resolve_header(raw_row, "Bezeichnung"),
                "Kauf_am": _normalize_date(_resolve_header(raw_row, "Kauf_am")),
                "Verkauf_am": _normalize_date(_resolve_header(raw_row, "Verkauf_am")),
                "Verkaufspreis": _normalize_amount(_resolve_header(raw_row, "Verkaufspreis")),
                "Kaufpreis": _normalize_amount(_resolve_header(raw_row, "Kaufpreis")),
                # Werbungskosten auf 2 Nachkommastellen (Programm akzeptiert meist keine mehr)
                "Werbungskosten": _normalize_amount(
                    _resolve_header(raw_row, "Werbungskosten"), decimals=2
                ),
            }
            # Einträge mit Kaufpreis oder Verkaufspreis 0.00 überspringen
            if _is_zero(row["Kaufpreis"]) or _is_zero(row["Verkaufspreis"]):
                skipped += 1
                continue
            rows.append(row)

        if skipped:
            print(
                f"[INFO] {skipped} Einträge mit Kauf-/Verkaufspreis 0,00 übersprungen.")

    return rows


# ---------------------------------------------------------------------------
# Fenster finden
# ---------------------------------------------------------------------------


def find_app_window():
    """Findet das SteuerSparErklärung-Fenster; beendet das Skript bei Fehler."""
    from pywinauto import Application, findwindows

    handles = findwindows.find_windows(title_re=f"(?i).*{APP_TITLE_PATTERN}.*")
    if not handles:
        print(f"[FEHLER] Kein Fenster mit Titel '{APP_TITLE_PATTERN}' gefunden.")
        print("         SteuerSparErklärung 2025 öffnen und erneut starten.")
        sys.exit(1)

    # Bei mehreren Treffern das beste Hauptfenster wählen:
    # 1) enthält den bekannten Toolbar-Button "Anlage" oder einen Toolbar-Bereich
    # 2) fallback: erstes sichtbares Fenster
    app = None
    best_win = None

    for h in handles:
        try:
            candidate_app = Application(backend="uia").connect(handle=h)
            candidate_win = candidate_app.window(handle=h)
            if not candidate_win.exists(timeout=0.5):
                continue

            # Bevorzugt: Fenster mit Anlage-Button / Toolbar
            has_anlage = False
            try:
                if candidate_win.child_window(title="Anlage", control_type="Button").exists(timeout=0.2):
                    has_anlage = True
            except Exception:
                pass
            try:
                if candidate_win.child_window(control_type="ToolBar").exists(timeout=0.2):
                    has_anlage = True
            except Exception:
                pass

            if has_anlage:
                return candidate_app, candidate_win

            if best_win is None:
                app = candidate_app
                best_win = candidate_win
        except Exception:
            continue

    if app is not None and best_win is not None:
        return app, best_win

    # Letzter Fallback
    app = Application(backend="uia").connect(handle=handles[0])
    win = app.window(handle=handles[0])
    return app, win


# ---------------------------------------------------------------------------
# --inspect
# ---------------------------------------------------------------------------


def cmd_inspect():
    """Gibt die UI-Hierarchie des Hauptfensters aus (depth=5)."""
    _, win = find_app_window()
    print(f"Hauptfenster: {win.window_text()!r}\n")
    win.print_control_identifiers(depth=5)


def cmd_inspect_nav():
    """Gibt den linken Navigations-Bereich (NavFrameSSE) mit depth=8 aus."""
    _, win = find_app_window()
    nav = win.child_window(auto_id=_AUTO_ID_NAV, control_type="Custom")
    if not nav.exists(timeout=2):
        print("[FEHLER] NavFrameSSE nicht gefunden.")
        sys.exit(1)
    print(f"Nav-Bereich ({_AUTO_ID_NAV}):\n")
    nav.print_control_identifiers(depth=8)


def cmd_inspect_form():
    """Navigiert zu 'Private Veräußerungsgeschäfte', klickt 'Anlage' und
    gibt den Formular-Bereich (RedThreadContent) mit depth=10 aus.
    Zeigt außerdem den aktuellen Formulartitel VOR und NACH dem Anlage-Klick.
    """
    _, win = find_app_window()
    print(f"[INFO] Formulartitel vor Navigation: '{_get_form_title(win)}'")
    print("[INFO] Navigiere zu letztem Veräußerungsgeschäft-Eintrag ...")
    _navigate_to_verauesserung(win)
    print(f"[INFO] Formulartitel nach Navigation: '{_get_form_title(win)}'")
    print("[INFO] Klicke 'Anlage'-Button ...")
    _click_anlage_button(win)
    time.sleep(DELAY_ENTRY)
    print(f"[INFO] Formulartitel nach Anlage:    '{_get_form_title(win)}'")
    form = win.child_window(auto_id=_AUTO_ID_FORM, control_type="Custom")
    if not form.exists(timeout=2):
        print("[FEHLER] RedThreadContent nicht gefunden.")
        sys.exit(1)
    print(f"\nFormular-Bereich ({_AUTO_ID_FORM}):\n")
    form.print_control_identifiers(depth=10)


def _load_coords() -> dict | None:
    """Lädt kalibrierte Feldkoordinaten aus Datei."""
    try:
        if not COORDS_PATH.exists():
            return None
        data = json.loads(COORDS_PATH.read_text(encoding="utf-8"))
        if all(k in data for k in _COORD_KEYS):
            return data
    except Exception:
        pass
    return None


def _get_optional_coord(coords: dict | None, key: str) -> tuple[int, int] | None:
    """Liefert eine optionale kalibrierte Koordinate, falls sie vorhanden ist."""
    if not coords:
        return None
    value = coords.get(key)
    if not isinstance(value, list) or len(value) != 2:
        return None
    try:
        return int(value[0]), int(value[1])
    except Exception:
        return None


def _save_coords(data: dict):
    """Speichert kalibrierte Feldkoordinaten in Datei."""
    COORDS_PATH.parent.mkdir(parents=True, exist_ok=True)
    COORDS_PATH.write_text(json.dumps(
        data, ensure_ascii=False, indent=2), encoding="utf-8")


def _get_form_scope(win):
    """Liefert den Formular-Container, falls er vorhanden ist."""
    try:
        form = win.child_window(auto_id=_AUTO_ID_FORM, control_type="Custom")
        return form if form.exists(timeout=1) else win
    except Exception:
        return win


def _focus_main_window(win):
    """Bringt das Hauptfenster in den Vordergrund, damit Tastatureingaben ankommen."""
    try:
        win.set_focus()
        time.sleep(DELAY_CLICK)
        return
    except Exception:
        pass
    try:
        win.click_input()
        time.sleep(DELAY_CLICK)
    except Exception:
        pass


def _get_cursor_position() -> tuple[int, int]:
    """Liest die aktuelle Mausposition über die Win32-API aus."""

    class POINT(ctypes.Structure):
        _fields_ = [("x", ctypes.c_long), ("y", ctypes.c_long)]

    point = POINT()
    if not ctypes.windll.user32.GetCursorPos(ctypes.byref(point)):
        raise RuntimeError("Mausposition konnte nicht gelesen werden.")
    return point.x, point.y


def _mouse_click_win32(x: int, y: int):
    """Führt einen Maus-Klick an den gegebenen Screenkoordinaten aus (umgeht UIA-Deadlock)."""
    # Maus zu Position bewegen
    ctypes.windll.user32.SetCursorPos(x, y)
    time.sleep(0.05)

    # Linksklick: Down + Up
    ctypes.windll.user32.mouse_event(2, 0, 0, 0, 0)  # MOUSEEVENTF_LEFTDOWN = 2
    time.sleep(0.05)
    ctypes.windll.user32.mouse_event(4, 0, 0, 0, 0)  # MOUSEEVENTF_LEFTUP = 4
    time.sleep(0.05)


def _send_ctrl_a_ctrl_c_win32():
    """Sendet Ctrl+A dann Ctrl+C über Win32 keybd_event (zuverlässiger als pywinauto in Qt)."""
    VK_CONTROL = 0x11
    VK_A = 0x41
    VK_C = 0x43
    KEYEVENTF_KEYUP = 0x0002
    keybd = ctypes.windll.user32.keybd_event

    # Ctrl+A
    keybd(VK_CONTROL, 0, 0, 0)
    keybd(VK_A, 0, 0, 0)
    time.sleep(0.05)
    keybd(VK_A, 0, KEYEVENTF_KEYUP, 0)
    keybd(VK_CONTROL, 0, KEYEVENTF_KEYUP, 0)
    time.sleep(0.15)

    # Ctrl+C
    keybd(VK_CONTROL, 0, 0, 0)
    keybd(VK_C, 0, 0, 0)
    time.sleep(0.05)
    keybd(VK_C, 0, KEYEVENTF_KEYUP, 0)
    keybd(VK_CONTROL, 0, KEYEVENTF_KEYUP, 0)
    time.sleep(0.1)


def _get_hwnd_at(x: int, y: int) -> int:
    """Liefert das tiefste HWND an den gegebenen Screenkoordinaten."""

    class POINT(ctypes.Structure):
        _fields_ = [("x", ctypes.c_long), ("y", ctypes.c_long)]

    pt = POINT(x, y)
    hwnd = ctypes.windll.user32.WindowFromPoint(pt)
    # ChildWindowFromPoint liefert das tiefste Kind
    child = ctypes.windll.user32.RealChildWindowFromPoint(hwnd, pt)
    return child if child else hwnd


def _get_hwnd_class(hwnd: int) -> str:
    """Liefert den Klassennamen eines HWND."""
    if not hwnd:
        return ""
    buf = ctypes.create_unicode_buffer(256)
    ctypes.windll.user32.GetClassNameW(hwnd, buf, 256)
    return buf.value or ""


def _is_edit_hwnd_at(x: int, y: int) -> bool:
    """Prüft ob an (x,y) ein Fenster ist, das sich wie ein Edit-Feld verhält.

    Strategie: WindowFromPoint, dann WM_GETTEXTLENGTH senden.
    Auf der Übersichtsseite gibt es an dieser Stelle kein Edit-Control.
    Auf dem Eingabeformular sitzt dort ein Qt-QLineEdit.
    """

    class POINT(ctypes.Structure):
        _fields_ = [("x", ctypes.c_long), ("y", ctypes.c_long)]

    pt = POINT(x, y)
    parent = ctypes.windll.user32.WindowFromPoint(pt)
    if not parent:
        return False

    # Qt-Apps haben oft ein einziges Top-Level-HWND. ChildWindowFromPointEx
    # liefert ggf. das Kind-Fenster.
    CWP_SKIPDISABLED = 0x0002
    CWP_SKIPINVISIBLE = 0x0001
    # Koordinaten relativ zum Parent machen
    client_pt = POINT(x, y)
    ctypes.windll.user32.ScreenToClient(parent, ctypes.byref(client_pt))
    child = ctypes.windll.user32.ChildWindowFromPointEx(
        parent, client_pt, CWP_SKIPDISABLED | CWP_SKIPINVISIBLE
    )
    hwnd = child if child and child != parent else parent

    cls = _get_hwnd_class(hwnd)
    # In Qt-Apps: wenn es ein anderes HWND ist als das Hauptfenster, ist es ein Eingabefeld.
    # Auch prüfen: Klassennamen die auf Edit/LineEdit hindeuten.
    if "edit" in cls.lower() or "lineedit" in cls.lower():
        return True

    # Fallback: Vergleiche HWND-Anzahl unter dem Punkt.
    # Wenn WindowFromPoint != ChildWindowFromPointEx → verschachteltes Control → Edit.
    if child and child != parent:
        return True

    return False


def _detect_form_by_focus(win, x_field: int, y_field: int) -> bool:
    """Erkennt ob wir auf dem Formular sind, indem wir prüfen ob das fokussierte
    Fenster nach einem Klick ein Edit-Caret hat (GetGUIThreadInfo)."""
    # Klick auf die Feldposition
    _mouse_click_win32(x_field, y_field)
    time.sleep(0.3)

    # GetGUIThreadInfo für den Vordergrund-Thread abfragen
    class GUITHREADINFO(ctypes.Structure):
        _fields_ = [
            ("cbSize", ctypes.c_ulong),
            ("flags", ctypes.c_ulong),
            ("hwndActive", ctypes.c_void_p),
            ("hwndFocus", ctypes.c_void_p),
            ("hwndCapture", ctypes.c_void_p),
            ("hwndMenuOwner", ctypes.c_void_p),
            ("hwndMoveSize", ctypes.c_void_p),
            ("hwndCaret", ctypes.c_void_p),
            ("rcCaret", ctypes.c_long * 4),
        ]

    gui = GUITHREADINFO()
    gui.cbSize = ctypes.sizeof(GUITHREADINFO)

    # Thread-ID des Vordergrund-Fensters
    fg = ctypes.windll.user32.GetForegroundWindow()
    tid = ctypes.windll.user32.GetWindowThreadProcessId(fg, None)

    if not ctypes.windll.user32.GetGUIThreadInfo(tid, ctypes.byref(gui)):
        return False

    # GUI_CARETBLINKING = 0x0001 in flags, oder hwndCaret != 0
    GUI_CARETBLINKING = 0x0001
    has_caret = bool(gui.hwndCaret) or (gui.flags & GUI_CARETBLINKING)
    return has_caret


def _read_clipboard() -> str:
    """Liest den aktuellen Text aus der Windows-Zwischenablage (Win32 API)."""
    CF_UNICODETEXT = 13
    user32 = ctypes.windll.user32
    kernel32 = ctypes.windll.kernel32

    if not user32.OpenClipboard(0):
        return ""
    try:
        handle = user32.GetClipboardData(CF_UNICODETEXT)
        if not handle:
            return ""
        ptr = kernel32.GlobalLock(handle)
        if not ptr:
            return ""
        try:
            return ctypes.wstring_at(ptr)
        finally:
            kernel32.GlobalUnlock(handle)
    finally:
        user32.CloseClipboard()


_CLIPBOARD_SENTINEL = "__STEUER_IMPORT_SENTINEL__"


def _set_clipboard(text: str) -> bool:
    """Schreibt Text in die Windows-Zwischenablage (Win32 API)."""
    CF_UNICODETEXT = 13
    GMEM_MOVEABLE = 0x0002
    user32 = ctypes.windll.user32
    kernel32 = ctypes.windll.kernel32

    encoded = (text + "\0").encode("utf-16-le")
    byte_count = len(encoded)

    h_mem = kernel32.GlobalAlloc(GMEM_MOVEABLE, byte_count)
    if not h_mem:
        return False
    ptr = kernel32.GlobalLock(h_mem)
    if not ptr:
        kernel32.GlobalFree(h_mem)
        return False
    ctypes.memmove(ptr, encoded, byte_count)
    kernel32.GlobalUnlock(h_mem)

    if not user32.OpenClipboard(0):
        kernel32.GlobalFree(h_mem)
        return False
    try:
        user32.EmptyClipboard()
        user32.SetClipboardData(CF_UNICODETEXT, h_mem)
    finally:
        user32.CloseClipboard()
    return True


def _uia_call(func, *args, timeout: float = 4.0, **kwargs):
    """Führt einen UIA-Aufruf in einem Daemon-Thread mit Timeout aus.

    Gibt das Ergebnis zurück, oder löst TimeoutError aus, wenn der Aufruf
    länger als 'timeout' Sekunden dauert (UIA-Deadlock-Schutz).
    Der blockierte Thread läuft als Daemon-Thread weiter und schadet nicht.
    """
    result = [None]
    exc = [None]
    done = threading.Event()

    def target():
        try:
            result[0] = func(*args, **kwargs)
        except Exception as e:
            exc[0] = e
        finally:
            done.set()

    t = threading.Thread(target=target, daemon=True)
    t.start()
    if done.wait(timeout):
        if exc[0] is not None:
            raise exc[0]
        return result[0]
    raise TimeoutError(f"UIA call timed out after {timeout}s: {func}")


def _has_named_field(scope, name: str, found_index: int = 0) -> bool:
    """Prüft, ob ein benanntes Eingabefeld im aktuellen Scope sichtbar ist."""
    for ctrl_type in ("Edit", "Spinner", "ComboBox"):
        try:
            ctrl = scope.child_window(
                title=name, control_type=ctrl_type, found_index=found_index
            )
            # Kein exists()-Check: blockiert.
            # Direkt versuchen auf das Control zuzugreifen.
            try:
                _ = ctrl.window_text()  # Test ob Control erreichbar ist
                return True
            except Exception:
                pass
        except Exception:
            pass
    return False


def _click_footer_button(win, title: str) -> bool:
    """Klickt einen Footer-Button wie 'Weiter' oder 'Zurück'.

    Kein _focus_main_window(): set_focus/click_input kann deadlocken.
    """
    # Win32-Pfad wenn Koordinate kalibriert
    if title.lower() == "weiter":
        coords = _load_coords()
        pos = _get_optional_coord(coords, "footer_weiter")
        if pos is not None:
            x, y = int(pos[0]), int(pos[1])
            _mouse_click_win32(x, y)
            time.sleep(DELAY_CLICK)
            print(f"  [INFO] Footer 'Weiter' per Win32 bei ({x},{y}) geklickt")
            return True

    # UIA-Fallback (kein focus, kein exists)
    try:
        btn = win.child_window(title=title, control_type="Button")
        btn.click_input()
        time.sleep(DELAY_CLICK)
        print(f"  [INFO] Footer '{title}' per UIA geklickt")
        return True
    except Exception:
        pass
    return False


def _advance_from_werbungskosten_submenu(win) -> bool:
    """Versucht den Unterdialog per zweimaligem 'Weiter' zu verlassen.

    Nutzt ausschließlich Win32 für Button-Klicks, kein UIA.
    """
    coords = _load_coords()
    pos = _get_optional_coord(coords, "footer_weiter")

    # Erster "Weiter"-Klick
    print("  [INFO] Erster 'Weiter'-Klick")
    _click_footer_button(win, "Weiter")
    time.sleep(DELAY_NAV)

    # Zweiter "Weiter"-Klick
    print("  [INFO] Zweiter 'Weiter'-Klick")
    _click_footer_button(win, "Weiter")
    time.sleep(DELAY_ENTRY)

    _UI_STATE["in_werbungskosten_submenu"] = False
    print("  [INFO] Aus Werbungskosten-Unterdialog per zweimal 'Weiter' fortgesetzt")
    return True


def _has_werbungskosten_input_fields(scope) -> bool:
    """True, wenn die Eingabefelder für Werbungskosten sichtbar sind."""
    return (
        _has_named_field(scope, "Art der Werbungskosten")
        or _has_named_field(scope, "Werbungskostenart")
        or _has_named_field(scope, "Betrag")
        or _has_named_field(scope, "Werbungskosten")
    )


def _click_werbungskosten_erfassen_button(scope) -> bool:
    """Klickt im Unterdialog einen '...erfassen'-Button, falls vorhanden.

    Warnung: child_window(title_re=...) kann hängen. Skip UIA-Suche.
    """
    # UIA-Suche nach "erfassen"-Button ist nach Win32-Klick oft unsicher.
    # Wird überjumpt - stattdessen _click_footer_button("Weiter") verwenden.
    return False


def _navigate_to_werbungskosten_submenu(win):
    """Öffnet den Unterdialog für Werbungskosten.

    Nutzt _click_footer_button (Win32 wenn kalibriert, sonst UIA ohne focus).
    """
    print("    [HEARTBEAT] öffne Werbungskosten via 'Weiter'")
    _click_footer_button(win, "Weiter")
    time.sleep(DELAY_NAV)
    _UI_STATE["in_werbungskosten_submenu"] = True
    print("    [HEARTBEAT] Werbungskosten-Dialog geöffnet")
    return win

    print(f"    [DEBUG] Klicke auf Werbungskosten-Dialog-Öffner bei ({x}, {y})")
    _mouse_click_win32(x, y)
    time.sleep(DELAY_NAV)
    _UI_STATE["in_werbungskosten_submenu"] = True
    print("    [HEARTBEAT] Werbungskosten-Dialog geöffnet")
    return win


def cmd_calibrate(seconds_per_field: int = 4, initial_delay: int = 5):
    """Hands-free Kalibrierung: Countdown je Feld, dann Mausposition erfassen."""
    _, win = find_app_window()
    print("[INFO] Kalibrierung gestartet.")
    print("[INFO] Kein Enter pro Feld nötig.")
    print("[INFO] Maus jeweils auf Feldmitte halten, wenn Countdown 1 erreicht.")
    print(f"[INFO] Start in {initial_delay} Sekunden ...")

    for s in range(initial_delay, 0, -1):
        print(f"  Start in {s}...")
        time.sleep(1)

    prompts = [
        ("dropdown", "Veräußerungsobjekt-Dropdown"),
        ("bezeichnung", "Bezeichnung"),
        ("verkauf_am", "Verkauf am"),
        ("verkauf_preis", "Verkaufspreis"),
        ("kauf_am", "Kauf am"),
        ("kauf_preis", "Kaufpreis"),
        ("werbungskosten_art", "Art der Werbungskosten"),
        ("werbungskosten", "Werbungskosten"),
        ("footer_weiter", "Footer-Button 'Weiter' (im Werbungskosten-Unterdialog)"),
        ("weiteres_veraeusserungsgeschaeft", "Button 'Weiteres Veräußerungsgeschäft erfassen'"),
    ]

    coords = {}
    print(f"[INFO] Aktueller Formulartitel: '{_get_form_title(win)}'")
    for key, label in prompts:
        if key == "werbungskosten_art":
            print("\n[INFO] Öffne Unterdialog für Werbungskosten ...")
            _navigate_to_werbungskosten_submenu(win)
            print(f"[INFO] Formulartitel jetzt: '{_get_form_title(win)}'")
        elif key == "footer_weiter":
            print(
                "\n[INFO] Kalibriere den 'Weiter'-Footer im aktuellen Werbungskosten-Unterdialog ...")
        elif key == "weiteres_veraeusserungsgeschaeft":
            print("\n[INFO] Verlasse Werbungskosten-Unterdialog für Button-Kalibrierung ...")
            if not _advance_from_werbungskosten_submenu(win):
                print(
                    "[WARNUNG] Unterdialog konnte nicht automatisch per zweimal 'Weiter' verlassen werden.")
                print(
                    "[WARNUNG] Bitte den richtigen Dialog manuell öffnen und Maus auf den Button halten.")
        print(f"\n[{label}] Maus positionieren ...")
        for s in range(seconds_per_field, 0, -1):
            print(f"  Aufnahme in {s}...")
            time.sleep(1)
        x, y = _get_cursor_position()
        coords[key] = [x, y]
        print(f"  -> gespeichert: ({x}, {y})")

    _save_coords(coords)
    print(f"\n[INFO] Kalibrierung gespeichert: {COORDS_PATH}")


# ---------------------------------------------------------------------------
# --dry-run / Import
# ---------------------------------------------------------------------------


def cmd_import(dry_run: bool, start_at: int):
    transactions = load_transactions(CSV_PATH)
    total = len(transactions)
    print(f"[INFO] {total} Transaktionen geladen.")

    if dry_run:
        print(f"[DRY-RUN] Zeige Einträge ab #{start_at}:\n")
        for i, tx in enumerate(transactions, 1):
            if i < start_at:
                continue
            print(
                f"  {i:2d}. {tx['Bezeichnung']:<12s}  "
                f"Kauf {tx['Kauf_am']}  "
                f"Verkauf {tx['Verkauf_am']}  "
                f"VP {tx['Verkaufspreis']}  "
                f"KP {tx['Kaufpreis']}  "
                f"WK {tx['Werbungskosten']}"
            )
        return

    _, win = find_app_window()

    # Für Win32-Driftkompensation beim Resume mit --start-at:
    # Bereits verarbeitete Einträge entsprechen näherungsweise start_at - 1.
    global _entry_count
    _entry_count = max(0, start_at - 1)
    print(f"[INFO] Verbunden mit: {win.window_text()!r}")
    print(f"[INFO] Drift-Offset initialisiert: entry_count={_entry_count}")
    print()
    print("ACHTUNG: Sicherstellen, dass SteuerSparErklärung geöffnet ist und")
    print("         der Fokus auf 'Private Veräußerungsgeschäfte' liegt.")
    print()
    input("Enter zum Starten, Strg+C zum Abbrechen ... ")
    print()

    for i, tx in enumerate(transactions, 1):
        if i < start_at:
            continue
        print(f"[{i:2d}/{total}] {tx['Bezeichnung']:<12s}  "
              f"Kauf {tx['Kauf_am']}  Verkauf {tx['Verkauf_am']}")
        try:
            _add_entry(win)
            _fill_entry(win, tx)
            _entry_count += 1
        except Exception as exc:
            print(f"  [FEHLER] {exc}")
            print(f"  Tipp: nach Korrektur mit '--start-at {i}' weitermachen.")
            sys.exit(1)

    print()
    print("[INFO] Import abgeschlossen. Bitte Daten in SteuerSpar prüfen.")


# ---------------------------------------------------------------------------
# Neuen Eintrag anlegen
# ---------------------------------------------------------------------------


def _get_form_title(win) -> str:
    """Liest den aktuellen Formulartitel aus dem ClientHeader."""
    try:
        lbl = win.child_window(auto_id=_AUTO_ID_FORM_HDR, control_type="Text")
        # Kein exists()-Check: blockiert nach Navigation.
        # Direkt versuchen zu lesen, Exception abfangen.
        try:
            text = lbl.window_text()
            if text:
                return text
        except Exception:
            pass
    except Exception:
        pass
    return ""


def _get_main_window_handle(win) -> int:
    """Liefert das native Handle des Hauptfensters, falls verfügbar."""
    try:
        return int(getattr(win, "handle", 0) or 0)
    except Exception:
        pass
    try:
        return int(getattr(win.element_info, "handle", 0) or 0)
    except Exception:
        pass
    return 0


def _get_form_title_subprocess(win, timeout: float = 1.0) -> str:
    """Liest den Formulartitel in einem separaten UIA-Prozess mit Timeout."""
    hwnd = _get_main_window_handle(win)
    if not hwnd:
        return ""

    probe = (
        "import sys;"
        "from pywinauto import Application;"
        f"auto_id={_AUTO_ID_FORM_HDR!r};"
        "hwnd=int(sys.argv[1]);"
        "app=Application(backend='uia').connect(handle=hwnd);"
        "win=app.window(handle=hwnd);"
        "lbl=win.child_window(auto_id=auto_id, control_type='Text');"
        "text=(lbl.window_text() or '').strip();"
        "sys.stdout.write(text)"
    )

    try:
        proc = subprocess.Popen(
            [sys.executable, "-c", probe, str(hwnd)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except Exception:
        return ""

    try:
        stdout, _stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
        except Exception:
            pass
        try:
            proc.communicate(timeout=0.2)
        except Exception:
            pass
        return ""
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
        return ""

    return (stdout or "").strip()


def _get_window_text_native(hwnd: int) -> str:
    """Liest den Fenstertitel eines nativen Win32-Handles."""
    if not hwnd:
        return ""

    length = ctypes.windll.user32.GetWindowTextLengthW(hwnd)
    buffer = ctypes.create_unicode_buffer(length + 1)
    ctypes.windll.user32.GetWindowTextW(hwnd, buffer, length + 1)
    return (buffer.value or "").strip()


def _collect_descendant_texts_native(root_hwnd: int, max_depth: int = 6) -> list[str]:
    """Sammelt sichtbare Texte nativer Child-Handles ohne UIA."""
    texts = []
    seen: set[int] = set()

    enum_proc = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_void_p, ctypes.c_void_p)

    def visit(hwnd: int, depth: int) -> None:
        if not hwnd or hwnd in seen or depth > max_depth:
            return
        seen.add(hwnd)

        text = _get_window_text_native(hwnd)
        if text:
            texts.append(text)

        @enum_proc
        def callback(child_hwnd, lparam):
            visit(int(child_hwnd), depth + 1)
            return True

        ctypes.windll.user32.EnumChildWindows(hwnd, callback, 0)

    visit(root_hwnd, 0)
    return texts


def _get_form_title_native(win) -> str:
    """Ermittelt den wahrscheinlichsten Formtitel rein über Win32-Handles."""
    hwnd = _get_main_window_handle(win)

    if not hwnd:
        return ""

    candidates = []
    for text in _collect_descendant_texts_native(hwnd):
        norm = _normalize_ui_text(text)
        if not norm:
            continue
        if "steuerspar" in norm:
            continue
        if "uebersicht" in norm or _looks_like_veraeusserung_text(norm) or "anlage" in norm or "analge" in norm:
            candidates.append(text)

    if not candidates:
        return ""

    candidates.sort(key=lambda item: (
        "uebersicht" not in _normalize_ui_text(item), len(item)), reverse=True)
    return candidates[0]


def _get_form_text_candidates_native(win) -> list[str]:
    """Liefert relevante native Texte aus dem Formularbereich für Header-Matching."""
    hwnd = _get_main_window_handle(win)

    if not hwnd:
        return []

    candidates = []
    for text in _collect_descendant_texts_native(hwnd):
        norm = _normalize_ui_text(text)
        if not norm:
            continue
        if "steuerspar" in norm:
            continue
        candidates.append(text)
    return candidates


def _is_on_verauesserung_form(win) -> bool:
    """True wenn das rechte Panel gerade ein Veräußerungsgeschäft-Formular zeigt."""
    title = _get_form_title(win)
    norm = _normalize_ui_text(title)
    return _looks_like_veraeusserung_text(norm)


def _is_on_verauesserung_input_form(win) -> bool:
    """True wenn ein Veräußerungs-Eingabeformular (nicht Übersicht) aktiv ist."""
    title = _get_form_title(win)
    norm = _normalize_ui_text(title)
    if "uebersicht" in norm:
        return False
    return _looks_like_veraeusserung_text(norm)


def _is_expected_weiteres_result_title(title: str) -> bool:
    """Prüft, ob der Header nach 'Weiteres ...' wie das Eingabeformular aussieht."""
    norm = _normalize_ui_text(title)
    if not norm or "uebersicht" in norm:
        return False

    # Der native Win32-Textscan liefert den Suffix '(Anlage SO)' nicht immer sauber.
    # Für den Klick-Loop reicht daher ein plausibler Formular-Header, solange wir
    # sicher nicht mehr auf der Übersicht sind.
    has_veraeusserung = _looks_like_veraeusserung_text(norm)
    has_anlage = "anlage" in norm or "analge" in norm or " so" in norm or norm.endswith(
        "so)")
    has_index = bool(re.search(r"(^|\s)\d+[.)]?\s", norm)
                     ) or bool(re.search(r"\(\d+\)", norm))
    return has_veraeusserung and (has_anlage or has_index)


def _find_expected_weiteres_result_title_native(win) -> str:
    """Sucht den echten Formular-Header nach 'Weiteres ...' über alle nativen Texte."""
    best_match = ""
    for text in _get_form_text_candidates_native(win):
        norm = _normalize_ui_text(text)
        if not _is_expected_weiteres_result_title(text):
            continue

        # Bevorzuge echte Header wie '4. Veräußerungsgeschäft (Anlage SO)'.
        score = 0
        if re.search(r"(^|\s)\d+[.)]?\s", norm):
            score += 3
        if "anlage" in norm or "analge" in norm:
            score += 3
        if " so" in norm or norm.endswith("so)"):
            score += 2
        if _looks_like_veraeusserung_text(norm):
            score += 2
        score += min(len(norm), 120) / 120.0

        if not best_match:
            best_match = text
            best_score = score
            continue

        if score > best_score:
            best_match = text
            best_score = score

    return best_match


def _normalize_ui_text(text: str) -> str:
    """Normalisiert UI-Texte für robuste Vergleiche trotz Umlaut-/Encoding-Problemen."""
    value = (text or "").lower()
    replacements = {
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "ß": "ss",
        "�": "a",
    }
    for old, new in replacements.items():
        value = value.replace(old, new)
    return " ".join(value.split())


def _looks_like_veraeusserung_text(norm_text: str) -> bool:
    """Unscharfer Match für Veräußerungsgeschäfte bei defekter Zeichencodierung."""
    norm = norm_text or ""
    return ("ver" in norm and "gesch" in norm) or ("verausserung" in norm)


def _is_private_veraeusserung_title(title: str) -> bool:
    """Prüft, ob ein Nav-Titel auf 'Private Veräußerungsgeschäfte' passt."""
    norm = _normalize_ui_text(title)
    return "private" in norm and _looks_like_veraeusserung_text(norm)


def _is_private_veraeusserung_entry_one_title(title: str) -> bool:
    """Prüft, ob ein tieferer Nav-Knoten '(N)' für Veräußerungsgeschäfte passt."""
    norm = _normalize_ui_text(title)
    # akzeptiert (1), (2), ...
    has_index = bool(re.search(r"\(\d+\)", norm))
    return "private" in norm and has_index and _looks_like_veraeusserung_text(norm)


def _activate_nav_node(node) -> bool:
    """Aktiviert einen Nav-TreeItem mit Tastatureingaben (kein UIA-Klick)."""
    from pywinauto.keyboard import send_keys

    print("    [HEARTBEAT] in _activate_nav_node, try minimal approach")
    try:
        print("    [HEARTBEAT] vor send_keys ESC")
        send_keys('{ESC}')  # {ESCAPE} ist kein gültiger pywinauto-Keyname
        time.sleep(0.2)
        print("    [HEARTBEAT] vor send_keys ENTER (ohne focus)")
        send_keys('{ENTER}')
        print("    [HEARTBEAT] nach send_keys ENTER")
    except Exception as e:
        print(f"    [ERROR] send_keys failed: {e}")
        return False

    print(f"    [HEARTBEAT] vor time.sleep({DELAY_CLICK})")
    time.sleep(DELAY_CLICK)
    print(f"    [HEARTBEAT] nach time.sleep({DELAY_CLICK})")
    return True


def _click_nav_private_veraeusserung(win) -> bool:
    """Klickt den Nav-TreeItem für 'Private Veräußerungsgeschäfte' robust."""
    print("    [HEARTBEAT] _click_nav_private_veraeusserung START")
    print("    [HEARTBEAT] vor child_window(auto_id=_AUTO_ID_NAV_TREE)")
    tree = win.child_window(auto_id=_AUTO_ID_NAV_TREE, control_type="Tree")
    print("    [HEARTBEAT] nach child_window(auto_id=_AUTO_ID_NAV_TREE)")

    # 1) Direkter Versuch mit erwartetem Titel
    # KEINE exists()-Abfrage: die blockiert nach mehrfacher Navigation.
    # Stattdessen: direkt versuchen zu klicken und Exception abfangen.
    try:
        print("    [HEARTBEAT] vor tree.child_window(title exakt)")
        node = tree.child_window(title=_NAV_TITLE_VERAUESSERUNG,
                                 control_type="TreeItem")
        print(
            "    [HEARTBEAT] nach tree.child_window(title exakt), direkt vor _activate_nav_node")
        result = _activate_nav_node(node)
        if result:
            print("  [NAV] Treffer über Exaktitel.")
            print("    [HEARTBEAT] _click_nav_private_veraeusserung END (True)")
            return True
    except Exception as e:
        print(f"    [ERROR] Exaktitel-Versuch exception: {e}")
        pass

    # 1b) Regex-Fallback (robust gegen Umlaute/Mojibake)
    try:
        print("    [HEARTBEAT] vor tree.child_window(title_re)")
        node = tree.child_window(
            title_re=r"(?i)private.*ver.*gesch", control_type="TreeItem")
        print("    [HEARTBEAT] nach tree.child_window(title_re), direkt vor _activate_nav_node")
        result = _activate_nav_node(node)
        if result:
            print("  [NAV] Treffer über Regex private.*ver.*gesch.")
            print("    [HEARTBEAT] _click_nav_private_veraeusserung END (True)")
            return True
    except Exception as e:
        print(f"    [ERROR] Regex-Versuch exception: {e}")
        pass

    print("  [NAV] Kein passender TreeItem für Private Veräußerungsgeschäfte gefunden.")
    print("    [HEARTBEAT] _click_nav_private_veraeusserung END (False)")
    return False


def _click_nav_private_veraeusserung_entry_one(win) -> bool:
    """Klickt den tieferen '(N)'-Knoten unter Veräußerungsgeschäfte, falls vorhanden."""
    print("    [HEARTBEAT] _click_nav_private_veraeusserung_entry_one START")
    print("    [HEARTBEAT] vor child_window(auto_id=_AUTO_ID_NAV_TREE)")
    tree = win.child_window(auto_id=_AUTO_ID_NAV_TREE, control_type="Tree")
    print("    [HEARTBEAT] nach child_window(auto_id=_AUTO_ID_NAV_TREE)")

    # 1) Direkter Versuch auf den erwarteten Titel mit (N)
    # KEINE exists()-Abfrage: die blockiert nach mehrfacher Navigation.
    # Stattdessen: direkt versuchen zu klicken und Exception abfangen.
    try:
        # Häufigster Fall: genau ein Eintrag
        print("    [HEARTBEAT] vor tree.child_window(title='(1)')")
        node = tree.child_window(
            title=f"{_NAV_TITLE_VERAUESSERUNG} (1)", control_type="TreeItem")
        print("    [HEARTBEAT] nach tree.child_window, direkt vor _activate_nav_node")
        result = _activate_nav_node(node)
        if result:
            print("  [NAV] Treffer über Exaktitel '(1)'.")
            print("    [HEARTBEAT] _click_nav_private_veraeusserung_entry_one END (True)")
            return True
    except Exception as e:
        print(f"    [ERROR] Exaktitel-Versuch exception: {e}")
        pass

    # 2) Regex-Fallback auf denselben Knoten, aber mit beliebiger Zahl
    try:
        print("    [HEARTBEAT] vor tree.child_window(title_re='(N)')")
        node = tree.child_window(
            title_re=r"(?i)^private.*ver.*gesch.*\(\d+\)$",
            control_type="TreeItem",
        )
        print("    [HEARTBEAT] nach tree.child_window(regex), direkt vor _activate_nav_node")
        result = _activate_nav_node(node)
        if result:
            print("  [NAV] Treffer über Regex '(N)'.")
            print("    [HEARTBEAT] _click_nav_private_veraeusserung_entry_one END (True)")
            return True
    except Exception as e:
        print(f"    [ERROR] Regex-Versuch exception: {e}")
        pass

    print("  [NAV] Kein tiefer '(1)'-Knoten gefunden.")
    print("    [HEARTBEAT] _click_nav_private_veraeusserung_entry_one END (False)")
    return False


def _click_anlage_button(win):
    """Klickt den Toolbar-Button 'Anlage' robust über mehrere Suchstrategien."""
    # 1) Exakte bekannte auto_id
    try:
        btn = win.child_window(auto_id=_AUTO_ID_BTN_ANLAGE, control_type="Button")
        if btn.exists(timeout=0.6):
            btn.click_input()
            return
    except Exception:
        pass

    # 2) Button mit sichtbarem Titel innerhalb der Toolbar
    try:
        toolbar = win.child_window(control_type="ToolBar")
        btn = toolbar.child_window(title="Anlage", control_type="Button")
        if btn.exists(timeout=0.6):
            btn.click_input()
            return
    except Exception:
        pass

    # 3) Global im Fenster nach Titel suchen
    try:
        btn = win.child_window(title="Anlage", control_type="Button")
        if btn.exists(timeout=0.6):
            btn.click_input()
            return
    except Exception:
        pass

    # 4) Letzter Fallback: über alle Buttons iterieren und auto_id-Muster prüfen
    try:
        for btn in win.descendants(control_type="Button"):
            try:
                aid = btn.element_info.automation_id or ""
                name = (btn.window_text() or "").strip()
                if aid.endswith(".tb_anlage") or aid == "tb_anlage" or name == "Anlage":
                    btn.click_input()
                    return
            except Exception:
                continue
    except Exception:
        pass

    raise RuntimeError(
        "Anlage-Button nicht gefunden (Toolbar/Title/auto_id Fallbacks).")


# Anzahl bereits erfasster Einträge (für vertikalen Button-Offset-Ausgleich)
_entry_count = 0
_last_weiteres_delta = 0


def _scroll_at(x: int, y: int, clicks: int = 5, direction: str = "down"):
    """Scrollt per Mausrad an der gegebenen Bildschirmposition.

    clicks: Anzahl Scroll-Schritte (je 120 Einheiten = ein Notch).
    direction: "down" oder "up".
    """
    MOUSEEVENTF_WHEEL = 0x0800
    WHEEL_DELTA = 120
    delta = -WHEEL_DELTA if direction == "down" else WHEEL_DELTA

    ctypes.windll.user32.SetCursorPos(x, y)
    time.sleep(0.05)
    for _ in range(clicks):
        # mouse_event dwData ist signed DWORD; ctypes akzeptiert negative Werte
        ctypes.windll.user32.mouse_event(MOUSEEVENTF_WHEEL, 0, 0, delta, 0)
        time.sleep(0.05)
    time.sleep(0.3)


def _click_weiteres_win32(win) -> bool:
    """Klickt 'Weiteres Veräußerungsgeschäft erfassen' per Win32 (kein UIA, deadlockt nie).

    Verwendet kalibrierte Koordinate aus steuer_coords.json.
    Versucht bis zu 20 Klicks (verschiedene Offsets, dann Wiederholungen)
    bevor blind fortgefahren wird.
    """
    global _last_weiteres_delta
    coords = _load_coords()
    pos = _get_optional_coord(coords, "weiteres_veraeusserungsgeschaeft")
    if pos is None:
        print("  [WARN] Keine Koordinate für 'weiteres_veraeusserungsgeschaeft' kalibriert.")
        print("  [WARN] Bitte 'python import_steuer.py --calibrate' ausführen.")
        return False
    x, y_base = int(pos[0]), int(pos[1])

    # Bei vielen Einträgen muss die Übersichtsseite nach unten gescrollt werden,
    # damit der Button sichtbar wird.  Scroll-Menge wächst mit Anzahl Einträge.
    if _entry_count > 0:
        scroll_clicks = 5 + _entry_count * 3
        # Scrolle in der Mitte des Formularbereichs (x vom Button, y etwas oberhalb)
        scroll_y = max(y_base - 200, 100)
        print(
            f"  [INFO] Scrolle Übersicht nach unten ({scroll_clicks} Notches bei y={scroll_y})")
        _scroll_at(x, scroll_y, clicks=scroll_clicks, direction="down")
        time.sleep(0.5)

    # Der Button verschiebt sich nach unten bei mehr Einträgen.
    # Start beim letzten bekannten Offset, dann in 18er Schritten nach unten/oben.
    max_attempts = 20
    y_anchor = y_base + _last_weiteres_delta
    ordered_deltas = [i * 18 for i in range(max_attempts)]

    # Header ist per UIA/Win32 nicht lesbar (Qt custom-drawn).
    # Stattdessen: Nach jedem Klick das Dropdown-Feld anklicken und per
    # Clipboard prüfen, ob wir auf dem Eingabeformular sind.
    dropdown_pos = _get_optional_coord(coords, "dropdown")

    attempt = 0

    # Äußere Schleife: bis zu max_attempts Klick-Versuche insgesamt.
    # Innere Schleife: verschiedene Offsets pro Runde.
    while attempt < max_attempts:
        for delta in ordered_deltas:
            if attempt >= max_attempts:
                break

            y = y_anchor + delta
            attempt += 1
            print(
                f"  [INFO] Win32-Klick 'Weiteres ...' "
                f"bei ({x},{y}) [Versuch {attempt}/{max_attempts}, "
                f"entry={_entry_count}, base_y={y_base}, delta={delta}]"
            )
            _mouse_click_win32(x, y)
            settle_delay = DELAY_ENTRY + 1.5
            print(f"  [INFO] Warte auf Formularwechsel ({settle_delay:.1f}s) ...")
            time.sleep(settle_delay)

            if not dropdown_pos:
                # Kein Dropdown kalibriert → blind weitermachen
                _last_weiteres_delta = _last_weiteres_delta + delta
                _UI_STATE["in_werbungskosten_submenu"] = False
                print("  [INFO] Eingabeformular wird fortgesetzt (kein Dropdown kalibriert)")
                return True

            # Validierung per Caret-Erkennung:
            # Klick auf Bezeichnungsfeld, dann prüfen ob ein Text-Cursor (Caret) aktiv ist.
            # Auf der Übersichtsseite gibt es kein Editfeld → kein Caret.
            bez_pos = _get_optional_coord(coords, "bezeichnung") or dropdown_pos
            x_check, y_check = int(bez_pos[0]), int(bez_pos[1])
            has_caret = _detect_form_by_focus(win, x_check, y_check)
            print(
                f"  [INFO] Caret-Check bei ({x_check},{y_check}): {has_caret}")

            if has_caret:
                _last_weiteres_delta = _last_weiteres_delta + delta
                _UI_STATE["in_werbungskosten_submenu"] = False
                print(f"  [INFO] Eingabeformular bestätigt (Caret aktiv)")
                return True
            else:
                print("  [WARN] Kein Caret – noch auf Übersichtsseite")
                continue

    # Nach max_attempts ohne Bestätigung: trotzdem fortfahren.
    print(f"  [WARN] {attempt} Versuche ohne Bestätigung – fahre trotzdem fort (blind)")
    _UI_STATE["in_werbungskosten_submenu"] = False
    return True


def _click_weiteres_veraeusserungsgeschaeft(win):
    """Klickt den In-Form-Button 'Weiteres Veräußerungsgeschäft erfassen' via UIA.

    Nur verlässlich wenn UIA noch nicht deadlockt (typisch: erster Eintrag).
    Für Folgeeinträge nach Submenu-Exit _click_weiteres_win32() verwenden.
    """
    # Exakter Titel
    try:
        btn = win.child_window(title=_BTN_TEXT_WEITERES, control_type="Button")
        btn.click_input()
        _UI_STATE["in_werbungskosten_submenu"] = False
        print("  [INFO] 'Weiteres ... erfassen' per UIA-Titel geklickt")
        return True
    except Exception:
        pass

    # Regex-Titel
    try:
        btn = win.child_window(title_re=_BTN_TEXT_WEITERES_RE, control_type="Button")
        btn.click_input()
        _UI_STATE["in_werbungskosten_submenu"] = False
        print("  [INFO] 'Weiteres ... erfassen' per UIA-Regex geklickt")
        return True
    except Exception:
        pass

    print("  [INFO] 'Weiteres ... erfassen' per UIA nicht gefunden")
    return False


def _click_nav_item(win, title: str):
    """Klickt ein TreeItem im Nav-Baum anhand des exakten Titels."""
    tree = win.child_window(auto_id=_AUTO_ID_NAV_TREE, control_type="Tree")
    node = tree.child_window(title=title, control_type="TreeItem")
    if not node.exists(timeout=1.0):
        raise RuntimeError(f"Nav-Knoten '{title}' nicht gefunden.")
    try:
        # select() ist häufig stabiler/schneller als click_input() bei TreeItems
        node.select()
        return
    except Exception:
        pass
    node.click_input()


def _navigate_and_wait(win, title: str, expected_fragment: str = "", timeout: float = 4.0):
    """Klickt Nav-Knoten und wartet bis das Formular gewechselt hat.

    Prüft alle 0.3s ob der Formulartitel `expected_fragment` enthält.
    Gibt True zurück wenn bestätigt, False bei Timeout.
    """
    _click_nav_item(win, title)
    if not expected_fragment:
        time.sleep(DELAY_NAV)
        return True
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(0.3)
        if expected_fragment.lower() in _get_form_title(win).lower():
            return True
    return False


def _navigate_to_verauesserung(win):
    """Navigiert direkt auf den sichtbaren Zielpunkt im offenen Nav-Baum."""
    # Annahme: Baum ist bereits aufgeklappt. Daher nur die sichtbare '(N)'-Ebene
    # anklicken. Der Blattknoten darunter führt direkt in einen einzelnen Eintrag
    # und ist damit eine Ebene zu tief für "Weiteres ... erfassen".
    print("    [HEARTBEAT] vor _click_nav_private_veraeusserung_entry_one")
    if not _click_nav_private_veraeusserung_entry_one(win):
        raise RuntimeError(
            "Sichtbarer Zielknoten unter 'Private Veräußerungsgeschäfte' nicht gefunden."
        )
    print("    [HEARTBEAT] nach _click_nav_private_veraeusserung_entry_one, vor sleep")
    # Keine weitere UIA-Polling-Schleife: die hat das Steuerprogramm teils blockiert.
    time.sleep(DELAY_NAV)
    print("    [HEARTBEAT] nach sleep, vor UI-State reset")
    _UI_STATE["in_werbungskosten_submenu"] = False
    print("  [NAV] Zielknoten geklickt.")


def _add_entry(win):
    """Navigiert zu Veräußerungsgeschäfte und legt neuen Eintrag an."""
    # Keine UIA-Formtitel-Prüfung im Hot-Path: die kann nach Dialogwechsel deadlocken.
    # Erster Eintrag darf den sichtbaren In-Form-Button nutzen, Folgeeinträge gehen direkt
    # über den Win32-Pfad auf der Übersicht.
    if not _UI_STATE["in_werbungskosten_submenu"]:
        if _entry_count == 0 and _click_weiteres_veraeusserungsgeschaeft(win):
            time.sleep(DELAY_ENTRY)
            print("  [INFO] Formular nach Neu-Eintrag: In-Form-Button verwendet")
            return

        print("  [INFO] Nutze Win32-Pfad für 'Weiteres ...'")
        if _click_weiteres_win32(win):
            time.sleep(DELAY_ENTRY)
            print("  [INFO] Formular nach Neu-Eintrag: Win32-Pfad verwendet")
            return

    if _UI_STATE["in_werbungskosten_submenu"]:
        print("  [INFO] Noch im Werbungskosten-Unterdialog; versuche zweimal 'Weiter'")
        if _advance_from_werbungskosten_submenu(win):
            # Nach zweimal Weiter: UIA-COM ist in diesem Zustand unzuverlässig/deadlockt.
            # Ausschließlich Win32-Koordinaten nutzen (kein UIA).
            print("  [INFO] 'Weiter'-Pfad erfolgreich; Win32-Klick auf 'Weiteres ...'")
            time.sleep(DELAY_ENTRY)  # Formular laden lassen
            if _click_weiteres_win32(win):
                time.sleep(DELAY_ENTRY)
                print("  [INFO] Formular nach Neu-Eintrag: Win32-Pfad nach Dialog-Exit")
                return
            raise RuntimeError(
                "Win32-Klick auf 'Weiteres Verfäußerungsgeschäft' fehlgeschlagen. "
                "Bitte --calibrate ausführen und 'weiteres_veraeusserungsgeschaeft' kalibrieren."
            )
        else:
            print("  [INFO] 'Weiter'-Pfad nicht erfolgreich; falle auf Navigation zurück")

    # Nur wenn der In-Form-Pfad nicht greift, auf den offenen Zielpunkt im
    # Nav-Baum zurückgehen.
    # ABER: Mit Timeout-Guard, um Deadlock zu vermeiden
    print("    [HEARTBEAT] vor _navigate_to_verauesserung (mit timeout)")
    try:
        _navigate_to_verauesserung(win)
    except RuntimeError as e:
        print(f"    [ERROR] Navigation blockiert/timeout: {e}")
        # Fallback: Fenster neu-fokussieren und nochmal versuchen
        print("    [ERROR] Versuche Fenster-Neustart")
        _focus_main_window(win)
        time.sleep(1.0)
        try:
            _navigate_to_verauesserung(win)
        except RuntimeError:
            raise RuntimeError(
                "Auch nach Neustart konnte nicht zur Veräußerung navigiert werden.")

    # Primär den In-Form-Button nutzen. Falls er nach dem ersten Nav-Klick
    # noch nicht sichtbar ist, einmal erneut navigieren und nochmals versuchen.
    print("    [HEARTBEAT] vor erstem _click_weiteres_veraeusserungsgeschaeft")
    # UIA ist nach Navigation weiter unzuverlässig → sofort Win32 als primären Pfad
    if not _click_weiteres_veraeusserungsgeschaeft(win) and not _click_weiteres_win32(win):
        # Zweiter Versuch: nochmal navigieren + Win32
        _navigate_to_verauesserung(win)
        if not _click_weiteres_win32(win):
            raise RuntimeError(
                "Button 'Weiteres Veräußerungsgeschäft erfassen' nicht gefunden."
            )
    print("    [HEARTBEAT] button click success")
    time.sleep(DELAY_ENTRY)
    print("  [INFO] Formular nach Neu-Eintrag: Navigationspfad verwendet")


# ---------------------------------------------------------------------------
# Felder ausfüllen
# ---------------------------------------------------------------------------


def _fill_entry(win, tx: dict):
    """Füllt Felder bevorzugt rein positionsbasiert."""
    # Kein _focus_main_window() im Hot-Path: set_focus/click_input kann deadlocken.
    # _fill_by_positions klickt das erste Feld ohnehin per absolute Koordinate an.
    print("    [HEARTBEAT] _fill_entry start")
    scope = win

    print("    [HEARTBEAT] _fill_entry vor _fill_by_positions")
    if _fill_by_positions(win, scope, tx):
        print("    [HEARTBEAT] _fill_entry done")
        return

    raise RuntimeError("Felder konnten nicht befüllt werden.")


def _set_combo_value(scope, value: str, *names: str) -> bool:
    """Setzt eine ComboBox anhand möglicher Feldnamen."""
    from pywinauto.keyboard import send_keys

    for name in names:
        try:
            ctrl = scope.child_window(title=name, control_type="ComboBox")
            # Kein exists()-Check: blockiert.
            # Direkt versuchen zu setzen.
            try:
                ctrl.select(value)
                time.sleep(DELAY_FIELD)
            except Exception:
                pass

            # Prüfen, ob bereits gesetzt
            try:
                if value.lower() in (ctrl.window_text() or "").lower():
                    return True
            except Exception:
                pass

            # Letzter Versuch: direkt im Control tippen und bestätigen.
            try:
                ctrl.click_input()
                time.sleep(DELAY_FIELD)
                send_keys("^a{BACKSPACE}", pause=DELAY_FIELD)
                send_keys(value, pause=DELAY_FIELD, with_spaces=True)
                send_keys("{ENTER}", pause=DELAY_FIELD)
                time.sleep(DELAY_FIELD)
                if value.lower() in (ctrl.window_text() or "").lower():
                    return True
            except Exception:
                pass
        except Exception:
            pass
    return False


def _fill_by_positions(win, scope, tx: dict) -> bool:
    """Positionsbasierte Eingabe relativ zum Formular-Rechteck.

    Diese Methode klickt jedes Feld explizit an und tippt den Wert direkt ein.
    Das ist für diese Legacy-UI meist stabiler als jede Tab-Navigation.
    """
    try:
        print("    [HEARTBEAT] _fill_by_positions start")
        coords = _load_coords()
        if coords is not None:
            pos_dropdown = tuple(coords["dropdown"])
            pos_bez = tuple(coords["bezeichnung"])
            pos_v_date = tuple(coords["verkauf_am"])
            pos_v_price = tuple(coords["verkauf_preis"])
            pos_k_date = tuple(coords["kauf_am"])
            pos_k_price = tuple(coords["kauf_preis"])
            pos_cost_type = tuple(coords["werbungskosten_art"])
            pos_cost = tuple(coords["werbungskosten"])
        else:
            form_rect = scope.rectangle()
            w = max(1, form_rect.width())
            h = max(1, form_rect.height())

            def p(rx: float, ry: float) -> tuple[int, int]:
                return (int(form_rect.left + w * rx), int(form_rect.top + h * ry))

            pos_dropdown = p(0.64, 0.080)
            pos_bez = p(0.64, 0.105)
            pos_v_date = p(0.53, 0.155)
            pos_v_price = p(0.84, 0.155)
            pos_k_date = p(0.53, 0.182)
            pos_k_price = p(0.84, 0.182)
            pos_cost_type = p(0.60, 0.160)
            pos_cost = p(0.84, 0.160)

        # Veräußerungsobjekt zuerst per Klick/Typing setzen.
        print("    [HEARTBEAT] fill dropdown")
        _click_and_type_at(scope, pos_dropdown,
                           _VERAEUSSERUNGSOBJEKT_VALUE, confirm=True, tab_after=True)
        print("    [HEARTBEAT] fill bezeichnung")
        _click_and_type_at(scope, pos_bez, tx["Bezeichnung"], tab_after=True)
        print("    [HEARTBEAT] fill verkauf_am")
        _click_and_type_at(scope, pos_v_date, tx["Verkauf_am"], tab_after=True)
        print("    [HEARTBEAT] fill verkaufspreis")
        _click_and_type_at(scope, pos_v_price, tx["Verkaufspreis"], tab_after=True)
        print("    [HEARTBEAT] fill kauf_am")
        _click_and_type_at(scope, pos_k_date, tx["Kauf_am"], tab_after=True)
        print("    [HEARTBEAT] fill kaufpreis")
        _click_and_type_at(scope, pos_k_price, tx["Kaufpreis"], tab_after=True)

        # Werbungskosten-Unterdialog öffnen und wieder verlassen.
        # Nur wenn Werbungskosten > 0, werden Art und Betrag eingetragen.
        wk = tx["Werbungskosten"]
        has_werbungskosten = wk and wk not in ("0", "0,00", "0.00", "0,0", "0.0")
        print("    [HEARTBEAT] vor open werbungskosten submenu")
        cost_scope = _navigate_to_werbungskosten_submenu(win)
        if has_werbungskosten:
            print("    [HEARTBEAT] fill werbungskosten_art")
            _click_and_type_at(cost_scope, pos_cost_type,
                               _WERBUNGSKOSTEN_ART_VALUE, confirm=True, tab_after=True)
            print("    [HEARTBEAT] fill werbungskosten_betrag")
            _click_and_type_at(cost_scope, pos_cost, wk, tab_after=True)
        else:
            print("    [HEARTBEAT] Werbungskosten=0 → keine Eingabe")
        print("    [HEARTBEAT] vor _advance_from_werbungskosten_submenu")
        _advance_from_werbungskosten_submenu(win)
        print("    [HEARTBEAT] _fill_by_positions done")
        return True
    except Exception as e:
        print(f"    [ERROR] _fill_by_positions Exception: {e}")
        return False


def _click_and_type_at(scope, pos: tuple[int, int], value: str, confirm: bool = False, tab_after: bool = False):
    """Klickt auf absolute Position und setzt den Feldwert."""
    from pywinauto import mouse
    from pywinauto.keyboard import send_keys

    mouse.click(button="left", coords=pos)
    time.sleep(DELAY_FIELD)
    try:
        send_keys("^a{BACKSPACE}", pause=DELAY_FIELD)
    except Exception:
        pass
    send_keys(value, pause=DELAY_FIELD, with_spaces=True)
    if confirm:
        send_keys("{ENTER}", pause=DELAY_FIELD)
    if tab_after:
        send_keys("{TAB}", pause=DELAY_FIELD)


def _set_veraeusserungsobjekt(scope) -> bool:
    """Setzt das Feld Veräußerungsobjekt auf 'Kryptowerte'."""
    return _set_combo_value(
        scope,
        _VERAEUSSERUNGSOBJEKT_VALUE,
        "Veräußerungsobjekt",
        "Veraeusserungsobjekt",
        "Objekt",
    )


def _fill_by_keyboard_sequence(win, scope, tx: dict) -> bool:
    """Füllt die Maske als lineare Sequenz per Tab-Navigation."""
    from pywinauto.keyboard import send_keys

    try:
        if not _set_veraeusserungsobjekt(scope):
            return False

        send_keys("{TAB}", pause=DELAY_FIELD)
        send_keys(tx["Bezeichnung"], pause=DELAY_FIELD, with_spaces=True)
        send_keys("{TAB}", pause=DELAY_FIELD)
        send_keys(tx["Verkauf_am"], pause=DELAY_FIELD, with_spaces=True)
        send_keys("{TAB}", pause=DELAY_FIELD)
        send_keys(tx["Verkaufspreis"], pause=DELAY_FIELD, with_spaces=True)
        send_keys("{TAB}", pause=DELAY_FIELD)
        send_keys(tx["Kauf_am"], pause=DELAY_FIELD, with_spaces=True)
        send_keys("{TAB}", pause=DELAY_FIELD)
        send_keys(tx["Kaufpreis"], pause=DELAY_FIELD, with_spaces=True)
        send_keys("{TAB}", pause=DELAY_FIELD)

        cost_scope = _navigate_to_werbungskosten_submenu(win)
        if not _set_combo_value(
            cost_scope,
            _WERBUNGSKOSTEN_ART_VALUE,
            "Art der Werbungskosten",
            "Werbungskostenart",
            "Art",
        ):
            return False

        send_keys("{TAB}", pause=DELAY_FIELD)
        send_keys(tx["Werbungskosten"], pause=DELAY_FIELD, with_spaces=True)
        send_keys("{TAB}", pause=DELAY_FIELD)
        return True
    except Exception:
        return False


def _fill_by_names(win, scope, tx: dict) -> bool:
    """Füllt Felder per Accessibility-Name / AutomationId aus."""
    try:
        if not _set_veraeusserungsobjekt(scope):
            return False
        _type_into(scope, tx["Bezeichnung"], "Bezeichnung")
        _type_into(scope, tx["Verkauf_am"], "Verkauf am")
        _type_into(scope, tx["Verkaufspreis"], "Verkaufspreis")
        _type_into(scope, tx["Kauf_am"], "Kauf am")
        _type_into(scope, tx["Kaufpreis"], "Kaufpreis")

        cost_scope = _navigate_to_werbungskosten_submenu(win)
        if not _set_combo_value(
            cost_scope,
            _WERBUNGSKOSTEN_ART_VALUE,
            "Art der Werbungskosten",
            "Werbungskostenart",
            "Art",
        ):
            return False

        if not _type_into(cost_scope, tx["Werbungskosten"], "Betrag"):
            return _type_into(cost_scope, tx["Werbungskosten"], "Werbungskosten")
        return True
    except Exception:
        return False


def _type_into(scope, value: str, name: str, found_index: int = 0) -> bool:
    """Klickt in ein benanntes Edit-Feld und tippt den Wert ein."""
    # Nur echte Eingabefelder berücksichtigen.
    # Text/Custom sind in dieser UI häufig Labels und keine editierbaren Controls.
    for ctrl_type in ("Edit", "Spinner"):
        try:
            ctrl = scope.child_window(
                title=name, control_type=ctrl_type, found_index=found_index
            )
            # Kein exists()-Check: blockiert.
            # Direkt versuchen zu klicken und zu tippen.
            try:
                ctrl.click_input()
                time.sleep(DELAY_FIELD)
                # set_edit_text ist bei klassischen Edit-Feldern am stabilsten.
                try:
                    ctrl.set_edit_text(value)
                except Exception:
                    ctrl.type_keys("^a", pause=DELAY_FIELD)
                    ctrl.type_keys(value, pause=DELAY_FIELD, with_spaces=True)
                time.sleep(DELAY_FIELD)
                return True
            except Exception:
                pass
        except Exception:
            pass
    return False


def _fill_by_tab(win, scope, tx: dict):
    """Fallback: nutzt Tab-Navigation nur, wenn keine anderen Wege greifen."""
    from pywinauto.keyboard import send_keys

    try:
        _focus_main_window(win)
        if not _set_veraeusserungsobjekt(scope):
            return False
        send_keys("{TAB}", pause=DELAY_FIELD)
        send_keys(tx["Bezeichnung"], pause=DELAY_FIELD, with_spaces=True)
        send_keys("{TAB}", pause=DELAY_FIELD)
        send_keys(tx["Verkauf_am"], pause=DELAY_FIELD, with_spaces=True)
        send_keys("{TAB}", pause=DELAY_FIELD)
        send_keys(tx["Verkaufspreis"], pause=DELAY_FIELD, with_spaces=True)
        send_keys("{TAB}", pause=DELAY_FIELD)
        send_keys(tx["Kauf_am"], pause=DELAY_FIELD, with_spaces=True)
        send_keys("{TAB}", pause=DELAY_FIELD)
        send_keys(tx["Kaufpreis"], pause=DELAY_FIELD, with_spaces=True)
        send_keys("{TAB}", pause=DELAY_FIELD)
        cost_scope = _navigate_to_werbungskosten_submenu(win)
        if not _set_combo_value(cost_scope, _WERBUNGSKOSTEN_ART_VALUE, "Art der Werbungskosten", "Werbungskostenart", "Art"):
            return False
        send_keys("{TAB}", pause=DELAY_FIELD)
        send_keys(tx["Werbungskosten"], pause=DELAY_FIELD, with_spaces=True)
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Einstiegspunkt
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Import Krypto-Veräußerungsgeschäfte in SteuerSparErklärung 2025",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--inspect", action="store_true",
        help="UI-Elemente des Programmfensters ausgeben und beenden (depth=5)",
    )
    parser.add_argument(
        "--inspect-nav", action="store_true",
        help="Gibt linken Nav-Baum (NavFrameSSE) mit depth=8 aus (zum Debuggen)",
    )
    parser.add_argument(
        "--inspect-form", action="store_true",
        help="Navigiert zu Veräußerungsgeschäfte, klickt 'Anlage', gibt Formular aus",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Geplante Eingaben anzeigen, ohne das Programm zu steuern",
    )
    parser.add_argument(
        "--calibrate", action="store_true",
        help="Interaktive Feld-Kalibrierung für stabile Koordinaten-Eingabe",
    )
    parser.add_argument(
        "--calibrate-seconds", type=int, default=4, metavar="N",
        help="Countdown pro Feld bei --calibrate (Standard: 4 Sekunden)",
    )
    parser.add_argument(
        "--start-at", type=int, default=1, metavar="N",
        help="Import ab Transaktion N starten (1-basiert, Standard: 1)",
    )
    args = parser.parse_args()

    if args.inspect:
        cmd_inspect()
    elif args.inspect_nav:
        cmd_inspect_nav()
    elif args.inspect_form:
        cmd_inspect_form()
    elif args.calibrate:
        cmd_calibrate(seconds_per_field=max(1, args.calibrate_seconds))
    else:
        cmd_import(dry_run=args.dry_run, start_at=args.start_at)


if __name__ == "__main__":
    main()
