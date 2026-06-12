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
            rows.append(row)

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
            if ctrl.exists(timeout=0.3):
                return True
        except Exception:
            pass
    return False


def _click_footer_button(win, title: str) -> bool:
    """Klickt einen Footer-Button wie 'Weiter' oder 'Zurück'."""
    if title.lower() == "weiter":
        coords = _load_coords()
        pos = _get_optional_coord(coords, "footer_weiter")
        if pos is not None:
            try:
                from pywinauto import mouse

                _focus_main_window(win)
                mouse.click(button="left", coords=pos)
                time.sleep(DELAY_CLICK)
                print("  [INFO] Footer 'Weiter' per Koordinate geklickt")
                return True
            except Exception:
                pass
        print("  [INFO] Footer 'Weiter' ohne Koordinate; UIA-Fallback aktiv")

    # Kein exists()-Check: blockiert nach mehrfacher Navigation.
    # Direkt click_input() und Exception abfangen.
    try:
        btn = win.child_window(title=title, control_type="Button")
        btn.click_input()
        time.sleep(DELAY_CLICK)
        return True
    except Exception:
        pass
    return False


def _advance_from_werbungskosten_submenu(win) -> bool:
    """Versucht den Unterdialog per zweimaligem 'Weiter' zu verlassen."""
    if not _click_footer_button(win, "Weiter"):
        return False
    time.sleep(DELAY_NAV)
    if not _click_footer_button(win, "Weiter"):
        return False
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
    """Klickt im Unterdialog einen '...erfassen'-Button, falls vorhanden."""
    for pattern in (
        r"(?i).*werbung.*erfassen.*",
        r"(?i).*kosten.*erfassen.*",
        r"(?i).*erfassen.*",
    ):
        try:
            btn = scope.child_window(title_re=pattern, control_type="Button")
            if btn.exists(timeout=0.5):
                btn.click_input()
                time.sleep(DELAY_CLICK)
                return True
        except Exception:
            pass
    return False


def _navigate_to_werbungskosten_submenu(win):
    """Öffnet den Unterdialog für Werbungskosten mit minimalen UIA-Aufrufen."""
    clicked_erfassen = _click_werbungskosten_erfassen_button(win)
    if not clicked_erfassen:
        _click_footer_button(win, "Weiter")
    time.sleep(DELAY_NAV)
    _UI_STATE["in_werbungskosten_submenu"] = True
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
    print(f"[INFO] Verbunden mit: {win.window_text()!r}")
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
            global _entry_count
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
        if lbl.exists(timeout=0.5):
            return lbl.window_text()
    except Exception:
        pass
    return ""


def _is_on_verauesserung_form(win) -> bool:
    """True wenn das rechte Panel gerade ein Veräußerungsgeschäft-Formular zeigt."""
    title = _get_form_title(win)
    norm = _normalize_ui_text(title)
    return _looks_like_veraeusserung_text(norm)


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
        print("    [HEARTBEAT] nach tree.child_window(title exakt), direkt vor _activate_nav_node")
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


def _click_weiteres_win32() -> bool:
    """Klickt 'Weiteres Veräußerungsgeschäft erfassen' per Win32 (kein UIA, deadlockt nie).
    
    Verwendet kalibrierte Koordinate aus steuer_coords.json.
    """
    coords = _load_coords()
    pos = _get_optional_coord(coords, "weiteres_veraeusserungsgeschaeft")
    if pos is None:
        print("  [WARN] Keine Koordinate für 'weiteres_veraeusserungsgeschaeft' kalibriert.")
        print("  [WARN] Bitte 'python import_steuer.py --calibrate' ausführen.")
        return False
    x, y = int(pos[0]), int(pos[1])
    print(f"  [INFO] Win32-Klick 'Weiteres ...' bei ({x},{y}) [entry={_entry_count}]")
    _mouse_click_win32(x, y)
    time.sleep(DELAY_CLICK)
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
    # Ohne Formtitel-Prüfung erst den In-Form-Button versuchen.
    if not _UI_STATE["in_werbungskosten_submenu"] and _click_weiteres_veraeusserungsgeschaeft(win):
        time.sleep(DELAY_ENTRY)
        print("  [INFO] Formular nach Neu-Eintrag: In-Form-Button verwendet")
        return

    if _UI_STATE["in_werbungskosten_submenu"]:
        print("  [INFO] Noch im Werbungskosten-Unterdialog; versuche zweimal 'Weiter'")
        if _advance_from_werbungskosten_submenu(win):
            # Nach zweimal Weiter: UIA-COM ist in diesem Zustand unzuverlässig/deadlockt.
            # Ausschließlich Win32-Koordinaten nutzen (kein UIA).
            print("  [INFO] 'Weiter'-Pfad erfolgreich; Win32-Klick auf 'Weiteres ...'")
            time.sleep(DELAY_ENTRY)  # Formular laden lassen
            if _click_weiteres_win32():
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
            raise RuntimeError("Auch nach Neustart konnte nicht zur Veräußerung navigiert werden.")

    # Primär den In-Form-Button nutzen. Falls er nach dem ersten Nav-Klick
    # noch nicht sichtbar ist, einmal erneut navigieren und nochmals versuchen.
    print("    [HEARTBEAT] vor erstem _click_weiteres_veraeusserungsgeschaeft")
    if not _click_weiteres_veraeusserungsgeschaeft(win):
        print("    [HEARTBEAT] first click failed, vor zweite _navigate_to_verauesserung")
        _navigate_to_verauesserung(win)
        print("    [HEARTBEAT] nach zweite _navigate_to_verauesserung, vor zweites _click_weiteres")
        if not _click_weiteres_veraeusserungsgeschaeft(win):
            raise RuntimeError(
                "Button 'Weiteres Veräußerungsgeschäft erfassen' nicht gefunden."
            )
    print("    [HEARTBEAT] button click success, vor final sleep")
    time.sleep(DELAY_ENTRY)
    print("  [INFO] Formular nach Neu-Eintrag: Navigationspfad verwendet")


# ---------------------------------------------------------------------------
# Felder ausfüllen
# ---------------------------------------------------------------------------


def _fill_entry(win, tx: dict):
    """Füllt Felder bevorzugt rein positionsbasiert."""
    _focus_main_window(win)
    scope = win

    if _fill_by_positions(win, scope, tx):
        return

    raise RuntimeError("Felder konnten nicht befüllt werden.")


def _set_combo_value(scope, value: str, *names: str) -> bool:
    """Setzt eine ComboBox anhand möglicher Feldnamen."""
    from pywinauto.keyboard import send_keys

    for name in names:
        try:
            ctrl = scope.child_window(title=name, control_type="ComboBox")
            if ctrl.exists(timeout=0.5):
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
        _click_and_type_at(scope, pos_dropdown,
                           _VERAEUSSERUNGSOBJEKT_VALUE, confirm=True, tab_after=True)
        _click_and_type_at(scope, pos_bez, tx["Bezeichnung"], tab_after=True)
        _click_and_type_at(scope, pos_v_date, tx["Verkauf_am"], tab_after=True)
        _click_and_type_at(scope, pos_v_price, tx["Verkaufspreis"], tab_after=True)
        _click_and_type_at(scope, pos_k_date, tx["Kauf_am"], tab_after=True)
        _click_and_type_at(scope, pos_k_price, tx["Kaufpreis"], tab_after=True)

        # Werbungskosten-Unterdialog erst öffnen, dann Art und Betrag setzen.
        cost_scope = _navigate_to_werbungskosten_submenu(win)
        _click_and_type_at(cost_scope, pos_cost_type,
                           _WERBUNGSKOSTEN_ART_VALUE, confirm=True, tab_after=True)
        _click_and_type_at(cost_scope, pos_cost, tx["Werbungskosten"], tab_after=True)
        return True
    except Exception:
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
            if ctrl.exists(timeout=0.4):
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
