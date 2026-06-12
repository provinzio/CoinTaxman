#!/usr/bin/env python3
"""
Import Krypto-Veräußerungsgeschäfte in SteuerSparErklärung 2025 via pywinauto.

Verwendung:
  python import_steuer.py --inspect        # Zeigt UI-Elemente (Toolbar/Nav/Form) an
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
import sys
import time
import argparse
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
    "werbungskosten",
]

# ---------------------------------------------------------------------------
# CSV laden
# ---------------------------------------------------------------------------


def load_transactions(csv_path: Path) -> list[dict]:
    """Liest Transaktionen; kürzt Datum auf DD.MM.YYYY, rundet Werbungskosten."""
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        rows = list(reader)

    for row in rows:
        row["Kauf_am"] = row["Kauf_am"].split()[0]
        row["Verkauf_am"] = row["Verkauf_am"].split()[0]
        # Werbungskosten auf 2 Nachkommastellen (Programm akzeptiert meist keine mehr)
        wk = float(row["Werbungskosten"].replace(",", "."))
        row["Werbungskosten"] = f"{wk:.2f}".replace(".", ",")

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


def _save_coords(data: dict):
    """Speichert kalibrierte Feldkoordinaten in Datei."""
    COORDS_PATH.parent.mkdir(parents=True, exist_ok=True)
    COORDS_PATH.write_text(json.dumps(
        data, ensure_ascii=False, indent=2), encoding="utf-8")


def cmd_calibrate(seconds_per_field: int = 4, initial_delay: int = 5):
    """Hands-free Kalibrierung: Countdown je Feld, dann Mausposition erfassen."""
    from pywinauto import mouse

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
        ("werbungskosten", "Werbungskosten"),
    ]

    coords = {}
    print(f"[INFO] Aktueller Formulartitel: '{_get_form_title(win)}'")
    for key, label in prompts:
        print(f"\n[{label}] Maus positionieren ...")
        for s in range(seconds_per_field, 0, -1):
            print(f"  Aufnahme in {s}...")
            time.sleep(1)
        x, y = mouse.get_position()
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
    return bool(title) and ("er\u00e4u\u00dferungsgesch" in title or "erausserungsgesch" in title.lower())


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


def _click_weiteres_veraeusserungsgeschaeft(win):
    """Klickt den In-Form-Button 'Weiteres Veräußerungsgeschäft erfassen'."""
    # 1) Exakter Titel
    try:
        btn = win.child_window(title=_BTN_TEXT_WEITERES, control_type="Button")
        if btn.exists(timeout=0.8):
            btn.click_input()
            return True
    except Exception:
        pass

    # 2) Regex auf Titel (robust gegen Umlaut-/Encoding-Unterschiede)
    try:
        btn = win.child_window(title_re=_BTN_TEXT_WEITERES_RE, control_type="Button")
        if btn.exists(timeout=0.8):
            btn.click_input()
            return True
    except Exception:
        pass

    # 3) Im Formularbereich nach passendem Button suchen
    try:
        form = win.child_window(auto_id=_AUTO_ID_FORM, control_type="Custom")
        for btn in form.descendants(control_type="Button"):
            try:
                text = (btn.window_text() or "").strip()
                if text == _BTN_TEXT_WEITERES:
                    btn.click_input()
                    return True
                if text and "Weiteres" in text and "erfassen" in text:
                    btn.click_input()
                    return True
            except Exception:
                continue
    except Exception:
        pass

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
    """Navigiert auf den Nav-Knoten 'Private Veräußerungsgeschäfte'.

    Diese Variante vermeidet teure Child-Enumeration im Tree (kann hängen)
    und arbeitet nur mit dem Parent-Knoten. Der Anlage-Button ist in diesem
    Kontext ausreichend, um einen neuen Eintrag anzulegen.
    """
    ok = _navigate_and_wait(win, _NAV_TITLE_VERAUESSERUNG,
                            expected_fragment="veräußer", timeout=4.0)
    if not ok:
        print(f"  [WARN] Formulartitel nach Nav-Klick: '{_get_form_title(win)}'")


def _add_entry(win):
    """Navigiert zu Veräußerungsgeschäfte und legt neuen Eintrag an."""
    # Wenn wir bereits auf einem Veräußerungsgeschäft-Formular sind,
    # zuerst den expliziten In-Form-Button verwenden.
    if _is_on_verauesserung_form(win):
        if not _click_weiteres_veraeusserungsgeschaeft(win):
            _click_anlage_button(win)
        time.sleep(DELAY_ENTRY)
        return

    # Navigation nötig
    _navigate_to_verauesserung(win)
    if not _click_weiteres_veraeusserungsgeschaeft(win):
        _click_anlage_button(win)
    time.sleep(DELAY_ENTRY)

    form_title = _get_form_title(win)
    print(f"  [INFO] Formular nach Neu-Eintrag: '{form_title}'")


# ---------------------------------------------------------------------------
# Felder ausfüllen
# ---------------------------------------------------------------------------


def _fill_entry(win, tx: dict):
    """Füllt Felder robust: zuerst positionsbasiert, dann Namens-/Tab-Fallback."""
    # Formular-Container für gezielten Scope nutzen
    try:
        form = win.child_window(auto_id=_AUTO_ID_FORM, control_type="Custom")
        scope = form if form.exists(timeout=1) else win
    except Exception:
        scope = win

    # Primär: robuste positionsbasierte Eingabe (stabil bei problematischer UIA-Erkennung).
    if _fill_by_positions(scope, tx):
        return

    # Fallback 1: namensbasierte Eingabe
    # Veräußerungsobjekt muss explizit auf "Kryptowerte" gesetzt werden.
    obj_ok = _set_veraeusserungsobjekt(scope)

    ok = _fill_by_names(scope, tx)
    if not (obj_ok and ok):
        # Fallback 2: reine Tastatur-Tab-Navigation
        print("  [INFO] Namensbasierte Eingabe unvollständig – Tab-Fallback.")
        _fill_by_tab(win, scope, tx)


def _fill_by_positions(scope, tx: dict) -> bool:
    """Positionsbasierte Eingabe relativ zum Formular-Rechteck.

    Diese Methode ist weniger elegant, aber in dieser Legacy-UI am zuverlässigsten.
    Sie setzt 'Kryptowerte' nur im ersten Dropdown und befüllt danach die 6 Felder.
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
            pos_cost = tuple(coords["werbungskosten"])
        else:
            form_rect = scope.rectangle()
            w = max(1, form_rect.width())
            h = max(1, form_rect.height())

            def p(rx: float, ry: float) -> tuple[int, int]:
                return (int(form_rect.left + w * rx), int(form_rect.top + h * ry))

            # Fallback-Feldzentren (nur wenn noch keine Kalibrierung existiert).
            pos_dropdown = p(0.64, 0.080)   # Veräußerungsobjekt
            pos_bez = p(0.64, 0.105)        # Bezeichnung
            pos_v_date = p(0.53, 0.155)     # Verkauf am
            pos_v_price = p(0.84, 0.155)    # Verkaufspreis
            pos_k_date = p(0.53, 0.182)     # Kauf am
            pos_k_price = p(0.84, 0.182)    # Kaufpreis
            pos_cost = p(0.84, 0.230)       # Werbungskosten

        _click_and_type_at(scope, pos_dropdown,
                           _VERAEUSSERUNGSOBJEKT_VALUE, confirm=True)
        _click_and_type_at(scope, pos_bez, tx["Bezeichnung"])
        _click_and_type_at(scope, pos_v_date, tx["Verkauf_am"])
        _click_and_type_at(scope, pos_v_price, tx["Verkaufspreis"])
        _click_and_type_at(scope, pos_k_date, tx["Kauf_am"])
        _click_and_type_at(scope, pos_k_price, tx["Kaufpreis"])
        _click_and_type_at(scope, pos_cost, tx["Werbungskosten"])
        return True
    except Exception:
        return False


def _click_and_type_at(scope, pos: tuple[int, int], value: str, confirm: bool = False):
    """Klickt auf absolute Position und setzt den Feldwert."""
    from pywinauto import mouse

    mouse.click(button="left", coords=pos)
    time.sleep(DELAY_FIELD)
    try:
        scope.type_keys("^a", pause=DELAY_FIELD)
    except Exception:
        pass
    scope.type_keys(value, pause=DELAY_FIELD, with_spaces=True)
    if confirm:
        scope.type_keys("{ENTER}", pause=DELAY_FIELD)


def _set_veraeusserungsobjekt(scope) -> bool:
    """Setzt das Feld Veräußerungsobjekt auf 'Kryptowerte'."""
    # 1) Direkte Suche über Feldname (nur ComboBox)
    try:
        ctrl = scope.child_window(
            title="Veräußerungsobjekt", control_type="ComboBox")
        if ctrl.exists(timeout=0.5):
            ctrl.click_input()
            time.sleep(DELAY_FIELD)
            try:
                ctrl.type_keys("^a", pause=DELAY_FIELD)
            except Exception:
                pass
            ctrl.type_keys(_VERAEUSSERUNGSOBJEKT_VALUE,
                           pause=DELAY_FIELD, with_spaces=True)
            ctrl.type_keys("{ENTER}", pause=DELAY_FIELD)
            return True
    except Exception:
        pass

    return False


def _fill_by_names(scope, tx: dict) -> bool:
    """Füllt Felder per Accessibility-Name / AutomationId aus."""
    results = [
        _type_into(scope, tx["Bezeichnung"], "Bezeichnung"),
        _type_into(scope, tx["Verkauf_am"], "Verkauf am"),
        # "zum Preis von" erscheint zweimal – erster ist Verkaufspreis
        (_type_into(scope, tx["Verkaufspreis"], "Verkaufspreis") or
         _type_into(scope, tx["Verkaufspreis"], "zum Preis von", found_index=0)),
        _type_into(scope, tx["Kauf_am"], "Kauf am"),
        # zweites "zum Preis von" = Kaufpreis
        (_type_into(scope, tx["Kaufpreis"], "Kaufpreis") or
         _type_into(scope, tx["Kaufpreis"], "zum Preis von", found_index=1)),
        _type_into(scope, tx["Werbungskosten"], "Werbungskosten"),
    ]
    return all(results)


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
    """Fallback: Tab-Navigation durch die Formularfelder.

    Reihenfolge laut Screenshot:
    Veräußerungsobjekt-Dropdown → Bezeichnung → Verkauf am → Verkaufspreis →
      Kauf am    → Kaufpreis   → Werbungskosten
    """
    def field(value: str):
        win.type_keys("^a", pause=DELAY_FIELD)
        win.type_keys(value, pause=DELAY_FIELD, with_spaces=True)
        win.type_keys("{TAB}", pause=DELAY_FIELD)

    # Veräußerungsobjekt nur im ersten Dropdown setzen
    win.type_keys("^a", pause=DELAY_FIELD)
    win.type_keys(_VERAEUSSERUNGSOBJEKT_VALUE,
                  pause=DELAY_FIELD, with_spaces=True)
    win.type_keys("{ENTER}", pause=DELAY_FIELD)
    win.type_keys("{TAB}", pause=DELAY_FIELD)

    # Bezeichnung explizit per Feldname setzen, damit sie nicht im Datumsfeld landet.
    bez_ok = _type_into(scope, tx["Bezeichnung"], "Bezeichnung")
    if bez_ok:
        # Nach direkter Eingabe einmal zum nächsten Feld (Verkauf am) weiter.
        win.type_keys("{TAB}", pause=DELAY_FIELD)
    else:
        # Letzter Fallback nur über Tab-Sequenz.
        field(tx["Bezeichnung"])

    field(tx["Verkauf_am"])
    field(tx["Verkaufspreis"])
    field(tx["Kauf_am"])
    field(tx["Kaufpreis"])
    field(tx["Werbungskosten"])


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
