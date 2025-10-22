"""Minimal Flask wrapper to expose the scheduling prototype via HTTP."""

import calendar
import io
import os
import sqlite3
from collections import defaultdict
from datetime import date, datetime, timedelta
from functools import wraps
from typing import Dict, List, Optional, Set, Tuple

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ModuleNotFoundError:
    pd = None  # type: ignore
    PANDAS_AVAILABLE = False

from flask import abort, g, Flask, redirect, render_template, request, send_file, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from src.db import (
    DEFAULT_ROTATION_PERIOD,
    add_clinic,
    add_clinic_seniority_rule,
    add_duty_type,
    add_staff,
    add_leave_request,
    delete_clinic,
    delete_clinic_seniority_rule,
    delete_staff,
    delete_leave_request,
    create_unit,
    create_unit_account,
    get_account_by_username,
    init_db,
    get_staff_by_id,
    get_unit_by_id,
    list_units,
    list_assignment_history,
    list_clinic_seniority_rules,
    list_clinics,
    list_duty_types,
    list_leave_requests,
    list_staff,
    replace_assignment_history,
    reorder_clinic,
    update_clinic_required_assistants,
    update_staff_preferences,
)
from src.solver_prototype import people_from_records, slots_from_records, solve_schedule


app = Flask(__name__)
app.secret_key = os.environ.get("OPTISHIFT_SECRET_KEY", "dev-secret")
init_db()

MONTH_OPTIONS = [
    (1, "Ocak"),
    (2, "Şubat"),
    (3, "Mart"),
    (4, "Nisan"),
    (5, "Mayıs"),
    (6, "Haziran"),
    (7, "Temmuz"),
    (8, "Ağustos"),
    (9, "Eylül"),
    (10, "Ekim"),
    (11, "Kasım"),
    (12, "Aralık"),
]
DEFAULT_YEAR_SPAN = 3
WEEKEND_HISTORY_MONTHS = 3

PLAN_TYPE_OPTIONS = [
    ("clinic", "Klinik Mesai Planı Oluştur"),
    ("nobet", "Nöbet Planı Oluştur"),
]

CLINIC_ROTATION_OPTIONS = [
    ("daily", "Günlük"),
    ("weekly", "Haftalık"),
    ("biweekly", "2 Haftalık"),
    ("monthly", "Aylık"),
]
CLINIC_ROTATION_LABELS = {value: label for value, label in CLINIC_ROTATION_OPTIONS}

SENIORITY_CHOICES = [
    ("kidemli", "Kıdemli"),
    ("ara", "Ara"),
    ("comez", "Çömez"),
]
SENIORITY_LABELS = {value: label for value, label in SENIORITY_CHOICES}

SUPPORTED_LANGUAGES = {
    "tr": "Türkçe",
    "en": "English",
}

TRANSLATIONS = {
    "en": {
        "Opti-Shift | Planlama": "Opti-Shift | Planning",
        "Planlama Panosu": "Planning Hub",
        "Aylık Görev Planı": "Monthly Duty Plan",
        "Klinik şefi olarak ekip mesai ve nöbetlerini dengeli şekilde paylaştırın. Seçili dönem: {month} {year}": "As the clinic lead, distribute shifts and duties evenly across your team. Selected period: {month} {year}",
        "Excel İndir": "Download Excel",
        "Plan Parametreleri": "Planning Parameters",
        "Plan Türü": "Plan Type",
        "İcap nöbeti uzmanlar arasında sırayla paylaştırılır.": "On-call duties are rotated evenly among specialists.",
        "Yıl": "Year",
        "Ay": "Month",
        "Plan Oluştur": "Generate Plan",
        "Plan Özeti": "Plan Summary",
        "Hazır": "Ready",
        "Durum": "Status",
        "Hedef": "Objective",
        "Dönem": "Period",
        "Plan": "Plan",
        "{month} {year}": "{month} {year}",
        "İcap Nöbeti Dağılımı": "On-call Duty Distribution",
        "Gece Nöbeti Dağılımı": "Night Duty Distribution",
        "Planı Onayla": "Approve Plan",
        "Onaylanan planın atamaları {period} dönemi için saklanır ve gelecek ayın adalet hesabında kullanılır.": "Approved assignments are stored for {period} and used in next month’s fairness calculation.",
        "Planı Onayla ve Kaydet": "Approve and Save Plan",
        "Günlük Plan Tablosu": "Daily Plan Table",
        "Tabloyu Excel olarak dışa aktarmak için üstteki Excel İndir seçeneğini kullanın.": "Use the Download Excel option above to export this table.",
        "Klinik Mesai Planı Oluştur": "Create Clinic Shift Plan",
        "Nöbet Planı Oluştur": "Create Duty Plan",
        "Planlama": "Planning",
        "Personel": "Staff",
        "Klinikler": "Clinics",
        "Nöbet Türleri": "Duty Types",
        "İzinler": "Leave",
        "Çıkış Yap": "Log Out",
        "Giriş Yap": "Log In",
        "Alpha v0.2": "Alpha v0.2",
        "Ocak": "January",
        "Şubat": "February",
        "Mart": "March",
        "Nisan": "April",
        "Mayıs": "May",
        "Haziran": "June",
        "Temmuz": "July",
        "Ağustos": "August",
        "Eylül": "September",
        "Ekim": "October",
        "Kasım": "November",
        "Aralık": "December",

        "İcap": "On-call duty",
        "Opti-Shift | Personel": "Opti-Shift | Staff",
        "Ekip Yönetimi": "Team Management",
        "Personel Paneli": "Staff Panel",
        "Klinik ekibinizi güncel tutun, görev sınırlarını ve kıdem kurallarını yönetin.": "Keep your clinic team up to date and manage duty limits with clear seniority rules.",
        "Toplam personel: {count}": "Total staff: {count}",
        "Yeni Personel Ekle": "Add New Staff",
        "Ad Soyad": "Full Name",
        "Örn: Dr. Ahmet Vural": "e.g., Dr. Ahmet Vural",
        "Ünvan": "Title",
        "Kıdem": "Seniority",
        "Kıdem seçin": "Select seniority",
        "Minimum Aylık Nöbet": "Minimum Monthly Duty",
        "Maksimum Aylık Nöbet": "Maximum Monthly Duty",
        "Örn: 2": "e.g., 2",
        "Örn: 6": "e.g., 6",
        "Personel Ekle": "Add Staff",
        "Personel Listesi": "Staff List",
        "{count} kayıt görüntüleniyor": "Showing {count} records",
        "Minimum Nöbet": "Minimum Duty",
        "Maksimum Nöbet": "Maximum Duty",
        "Minimum nöbet": "Minimum duty",
        "Maksimum nöbet": "Maximum duty",
        "İşlemler": "Actions",
        "Ekle": "Add",
        "Klinik Ekle": "Add Clinic",
        "Yeni Klinik Ekle": "Add New Clinic",
        "Alt sınır yok": "No lower limit",
        "Üst sınır yok": "No upper limit",
        "Kaydet": "Save",
        "Sil": "Delete",
        "Henüz kayıtlı personel bulunmuyor. Önce ekip arkadaşlarınızı ekleyin.": "No staff registered yet. Add your team members first.",
        "Kıdemli": "Senior",
        "Ara": "Intermediate",
        "Çömez": "Junior",
        "Uzm. Dr.": "Spec.",
        "Asst. Dr.": "Resident",
        "Opti-Shift | Klinikler": "Opti-Shift | Clinics",
        "Kadro Planlama": "Staffing",
        "Klinik Yönetimi": "Clinic Management",
        "Klinik tanımlarını, sorumlu uzmanları ve kıdem kurallarını buradan düzenleyin.": "Define clinics, responsible specialists, and seniority rules here.",
        "Toplam klinik: {count}": "Total clinics: {count}",
        "Gerekli Asistan Sayısı": "Required Residents",
        "Rotasyon Periyodu": "Rotation Period",
        "Sorumlu Uzman": "Lead Specialist",
        "Sorumlu: {name}": "Lead: {name}",
        "Uzman seçin": "Select a specialist",
        "Kayıtlı Klinikler": "Registered Clinics",
        "{count} klinik listeleniyor": "Listing {count} clinics",
        "Gerekli asistan: {count} • Rotasyon: {rotation}": "Required residents: {count} • Rotation: {rotation}",
        "Sorumlu uzman: {name}": "Lead specialist: {name}",
        "Yukarı": "Move Up",
        "Aşağı": "Move Down",
        "Gerekli Asistan": "Required Residents",
        "Güncelle": "Update",
        "Kıdem Kuralları": "Seniority Rules",
        "Kıdem bazlı zorunlu sayılar": "Mandatory counts by seniority",
        "{count} adet": "{count} items",
        "Henüz kural eklenmedi.": "No rules added yet.",
        "Henüz klinik eklenmedi. Önce ekip yapınızı oluşturun.": "No clinics defined yet. Build your structure first.",
        "Günlük": "Daily",
        "Haftalık": "Weekly",
        "2 Haftalık": "Bi-weekly",
        "Aylık": "Monthly",
        "Opti-Shift | İzinler": "Opti-Shift | Leave",
        "Planlama Dengesi": "Scheduling Balance",
        "İzin Kayıtları": "Leave Records",
        "Aktif izin kaydı: {count}": "Active leave entries: {count}",
        "Kayıtlı İzinler": "Registered Leave Records",
        "Başlangıç": "Start",
        "Bitiş": "End",
        "Sebep": "Reason",
        "İşlem": "Action",
        "Henüz izin kaydı bulunmuyor.": "No leave records yet.",
        "Yeni İzin Ekle": "Add New Leave",
        "Önce personel ekleyin. Personel olmadan izin oluşturulamaz.": "Add staff first. You cannot create leave without staff.",
        "Personel seçin": "Select staff",
        "Personel izinlerini takip ederek planlama motorunun güncel verilerle çalışmasını sağlayın.": "Track staff leave so the scheduling engine always runs on current data.",
        "Başlangıç Tarihi": "Start Date",
        "Bitiş Tarihi": "End Date",
        "Sebep (opsiyonel)": "Reason (optional)",
        "Örn: Kongre, Tatil": "e.g., Conference, Vacation",
        "İzin Kaydet": "Save Leave",
        "Bitiş tarihi dahil edilerek personel bu günlerde planlamaya alınmaz.": "The end date is inclusive; the staff member will not be scheduled on these days.",
        "Opti-Shift | Nöbet Türleri": "Opti-Shift | Duty Types",
        "Görev Tipleri": "Duty Types",
        "Nöbet Türleri": "Duty Types",
        "Nöbet ve mesai görev tanımlarını oluşturun, planlama motorunun gereksinimlerini belirleyin.": "Define duty and shift types to configure the planning engine.",
        "Toplam nöbet türü: {count}": "Total duty types: {count}",
        "Yeni Nöbet Türü": "New Duty Type",
        "Nöbet Adı": "Duty Name",
        "Örn: 16": "e.g., 16",
        "Örn: Dermatoloji": "e.g., Dermatology",
        "Örn: Gece Nöbeti": "e.g., Night Duty",
        "Süre (saat)": "Duration (hours)",
        "Kategori": "Category",
        "Mesai": "Day Shift",
        "Nöbet": "Duty",
        "Gerekli Personel": "Required Staff",
        "Bu görev icap nöbeti kalıbını kullansın": "Use on-call duty template for this assignment",
        "Nöbet Türü Ekle": "Add Duty Type",
        "Kayıtlı Nöbet Türleri": "Registered Duty Types",
        "{count} kayıt": "{count} entries",
        "Ad": "Name",
        "Henüz nöbet türü eklenmedi.": "No duty types added yet.",
        "Opti-Shift | Giriş": "Opti-Shift | Sign In",
        "Opti-Shift Yönetim Girişi": "Opti-Shift Admin Login",
        "Planlama paneline erişmek için sistem yöneticinizden aldığınız bilgileri kullanın.": "Use the credentials provided by your system administrator to access the planning console.",
        "Kullanıcı Adı": "Username",
        "örnek@klinik.com": "example@clinic.com",
        "Şifre": "Password",
        "Şifreniz": "Your password",
        "İlk giriş bilgilerinizi sistem yöneticiniz paylaşır. Sorun yaşarsanız destek ekibi ile iletişime geçin.": "Your administrator shares your initial credentials. Contact support if you have issues.",
        "Atanmamış": "Unassigned",
        "Excel çıktısı için pandas ve openpyxl kütüphaneleri gerekli.\nKurulum: pip install pandas openpyxl": "Excel export requires pandas and openpyxl.\nInstall: pip install pandas openpyxl",
        "Plan kaydedildi.": "Plan saved.",
        "Geçerli bir yıl ve ay seçin.": "Select a valid year and month.",
        "Plan oluşturulamadı.": "Plan could not be generated.",
        "Lütfen önce personel ekleyin. /personel sayfasından kayıt oluşturabilirsiniz.": "Please add staff first. You can create records from the /personel page.",
        "İcap nöbeti tanımı bulunamadı. /nöbetler sayfasından ekleyin.": "On-call duty definition not found. Add it on the /nobetler page.",
        "Planlama için en az bir klinik veya mesai görevi ekleyin. /klinikler ve /nöbetler sayfalarını kullanabilirsiniz.": "Add at least one clinic or duty definition. Use the /klinikler and /nobetler pages.",
        "Bu verilerle oluşturulacak slot bulunamadı. Klinik ve görev tanımlarınızı kontrol edin.": "No slots can be generated with the provided data. Check clinic and duty definitions.",
        "Çözüm bulunamadığı için tekrar cezası devre dışı bırakıldı; ardışık klinik atamaları oluşabilir.": "Repeat penalty disabled to find a solution; consecutive clinic assignments may occur.",
        "Kıdem gereksinimleri gevşetildi; klinik kadrosunu manuel olarak gözden geçirin.": "Seniority requirements relaxed; review clinic staffing manually.",
        "Planlama sırasında hata oluştu: {detay}": "An error occurred while scheduling: {detay}",
        "Excel çıktısı için pandas ve openpyxl kütüphaneleri gerekli.\nKurulum: pip install pandas openpyxl": "Excel export requires pandas and openpyxl.\nInstall: pip install pandas openpyxl",
        "Geçerli bir personel seçin.": "Select a valid staff member.",
        "Personel kaydı bulunamadı.": "Staff record not found.",
        "Yalnızca Asst. Dr. kayıtları güncellenebilir.": "Only Assistant Doctor records can be updated.",
        "Geçerli kıdem seçin.": "Select a valid seniority.",
        "Minimum nöbet sayısı maksimumdan büyük olamaz.": "Minimum duty count cannot exceed the maximum.",
        "Nöbet sınırları negatif olamaz.": "Duty limits cannot be negative.",
        "Lütfen ad soyad girin.": "Enter a full name.",
        "Geçerli ünvan seçin.": "Select a valid title.",
        "Nöbet sınırları yalnızca Asst. Dr. için girilebilir.": "Duty limits can only be set for Assistant Doctors.",
        "Bilinmeyen işlem tipi.": "Unknown action type.",
        "Geçerli bir izin kaydı seçin.": "Select a valid leave record.",
        "Başlangıç ve bitiş tarihlerini girin.": "Enter both start and end dates.",
        "Tarih formatları GGGG-AA-GG olmalıdır.": "Dates must use the YYYY-MM-DD format.",
        "Bitiş tarihi başlangıçtan önce olamaz.": "The end date cannot be earlier than the start date.",
        "Lütfen klinik adını girin.": "Enter a clinic name.",
        "Bu isimde bir klinik zaten mevcut.": "A clinic with this name already exists.",
        "Geçerli bir klinik seçin.": "Select a valid clinic.",
        "Sıralama güncellenemedi.": "Ordering could not be updated.",
        "Geçerli bir asistan sayısı girin.": "Enter a valid resident count.",
        "Geçerli bir kıdem seviyesi seçin.": "Select a valid seniority level.",
        "Kural adedi 1 veya daha büyük olmalıdır.": "Rule count must be at least 1.",
        "Geçerli bir kural seçin.": "Select a valid rule.",
        "Lütfen tüm alanları doldurun.": "Please fill out all fields.",
        "Süre alanı tam sayı olmalıdır.": "Duration must be an integer.",
        "Süre sıfırdan büyük olmalıdır.": "Duration must be greater than zero.",
        "Geçerli bir personel sayısı girin.": "Enter a valid staff count.",
        "Bu isimde bir nöbet türü zaten mevcut.": "A duty type with this name already exists.",
        "Planlama Dengesi": "Scheduling Balance",
        "Toplam nöbet türü: {count}": "Total duty types: {count}",
        "örnek@klinik.com": "example@clinic.com",
        "Şifreniz": "Your password",
        "Atanmamış": "Unassigned",
        "İcap Özeti": "On-call Summary",
        "Gece Nöbeti Özeti": "Night Duty Summary",
        "Tarih": "Date",
        "Personel": "Staff",
        "Başlangıç": "Start",
        "Bitiş": "End",
        "Sebep": "Reason",
        "İşlem": "Action",
        "Sorumlu Uzman": "Lead Specialist",
        "Uzman seçin": "Select a specialist",
        "Klinik Adı": "Clinic Name",
        "Gerekli Asistan": "Required Residents",
        "Güncelle": "Update",
        "İcap Nöbeti Dağılımı": "On-call Duty Distribution",
        "Gece Nöbeti Dağılımı": "Night Duty Distribution",
        "Hafta İçi Gün": "Weekday Days",
        "Hafta Sonu Gün": "Weekend Days",
        "Atanan Gün": "Assigned Days",
        "Toplam Saat": "Total Hours",
        "Atanan Görev": "Assigned Duties",
        "Hafta İçi": "Weekday",
        "Hafta Sonu": "Weekend",
        "Alt sınır yok": "No lower limit",
        "Üst sınır yok": "No upper limit",
        "Sorumlu: {name}": "Lead: {name}",
        "Minimum Limit": "Minimum Limit",
        "Maksimum Limit": "Maximum Limit",
        "Uzman sayısı: {count}": "Specialists: {count}",
        "Toplam gün: {count}": "Total days: {count}",
        "Dağılım:": "Distribution:",
        "=== İcap Nöbet Planı ===": "=== On-call Duty Plan ===",
        "- {person}: {assignments} gün (hafta içi {weekday}, hafta sonu {weekend}), toplam {hours} saat": "- {person}: {assignments} duties (weekdays {weekday}, weekends {weekend}), total {hours} hours",
        "- Uzmanlara görev atanmadı.": "- No specialists were assigned.",
        "Geçerli olmayan nöbet sınırları: {name} için minimum {minimum} maksimumdan büyük.": "Invalid duty limits: minimum {minimum} for {name} exceeds the maximum.",
        "Gece nöbeti için en az bir 'Asst. Dr.' gereklidir.": "Night duty requires at least one 'Asst. Dr.'.",
        "Gece nöbeti slotu oluşmadı.": "No night duty slots were generated.",
        "Gece nöbeti atamaları için çözüm bulunamadı: {detay}": "No solution found for night duty assignments: {detay}",
        "=== Gece Nöbeti Planı ===": "=== Night Duty Plan ===",
        "Asistan sayısı: {count}": "Residents: {count}",
        "Toplam görev: {count}": "Total duties: {count}",
        "- {person}: {assignments} görev (hafta içi {weekday}, hafta sonu {weekend}), toplam {hours} saat": "- {person}: {assignments} duties (weekdays {weekday}, weekends {weekend}), total {hours} hours",
        "- Asistanlara görev atanmadı.": "- No residents were assigned.",
        "Hafta sonu denge geçmişi gevşetildi; hafta sonu planlamasını manuel olarak doğrulayın.": "Weekend fairness history was relaxed; verify weekend coverage manually.",
        "Gece nöbeti tanımı bulunmuyor.": "Night duty definition not found.",
        "İcap nöbeti için en az bir 'Uzm. Dr.' gereklidir.": "On-call duty requires at least one 'Uzm. Dr.'.",
        "Ünite seçilmedi": "No unit selected",
    }
}


def get_locale() -> str:
    lang = session.get("language")
    if lang in SUPPORTED_LANGUAGES:
        return lang
    return "tr"


def translate(text: str, **kwargs) -> str:
    lang = get_locale()
    translations = TRANSLATIONS.get(lang, {})
    template = translations.get(text, text)
    try:
        return template.format(**kwargs)
    except (KeyError, ValueError):
        return template


def _(text: str, **kwargs) -> str:
    return translate(text, **kwargs)


def _safe_redirect_target(target: Optional[str]) -> Optional[str]:
    if target and target.startswith("/") and not target.startswith("//"):
        return target
    return None


def _current_unit() -> Optional[Dict[str, str]]:
    return getattr(g, "current_unit", None)


def _require_unit_id() -> int:
    unit = _current_unit()
    if not unit:
        abort(401)
    return int(unit["id"])


@app.before_request
def load_current_account() -> None:
    unit_id = session.get("unit_id")
    username = session.get("username")
    g.current_user = None
    g.current_unit = None
    g.current_language = get_locale()
    if unit_id is None:
        return
    unit_row = get_unit_by_id(unit_id)
    if unit_row is None:
        session.clear()
        return
    g.current_user = {"username": username or "", "unit_id": int(unit_row["id"])}
    g.current_unit = {"id": int(unit_row["id"]), "name": unit_row["name"]}


@app.context_processor
def inject_template_context():
    unit = _current_unit()
    user = getattr(g, "current_user", None)
    unit_name = unit["name"] if unit else None
    username = user["username"] if user else None
    return {
        "t": translate,
        "current_username": username,
        "current_clinic_name": unit_name,
        "current_unit_name": unit_name,
        "current_language": get_locale(),
        "language_options": list(SUPPORTED_LANGUAGES.items()),
    }


def login_required(view):
    @wraps(view)
    def wrapped_view(*args, **kwargs):
        if "unit_id" not in session:
            next_candidate = request.full_path if request.query_string else request.path
            next_hint = _safe_redirect_target(next_candidate.rstrip("?"))
            if next_hint:
                return redirect(url_for("login", next=next_hint))
            return redirect(url_for("login"))
        return view(*args, **kwargs)

    return wrapped_view


def _safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _previous_month(year: int, month: int) -> tuple[int, int]:
    """Return the year and month tuple for the previous calendar month."""
    if month <= 1:
        return year - 1, 12
    return year, month - 1


def _plan_period(year: int, month: int) -> str:
    """Return YYYY-MM formatted string for a plan period."""
    return f"{year:04d}-{month:02d}"


def _extract_clinic_id(slot_identifier: str) -> Optional[int]:
    """Parse clinic ID from slot identifier string."""
    if not slot_identifier.startswith("clinic_"):
        return None
    parts = slot_identifier.split("_", 2)
    if len(parts) < 2:
        return None
    try:
        return int(parts[1])
    except ValueError:
        return None


def _classify_day_type(day_value: date) -> str:
    """Classify a date as weekday or weekend."""
    return "weekend" if day_value.weekday() >= 5 else "weekday"


@app.route("/")
def index():
    if "unit_id" not in session:
        return redirect(url_for("login"))
    return redirect(url_for("planla"))

@app.route("/login", methods=["GET", "POST"])
def login():
    if "unit_id" in session:
        return redirect(url_for("planla"))
    error = None
    next_url = _safe_redirect_target(request.args.get("next"))
    if request.method == "POST":
        username = (request.form.get("username") or "").strip().lower()
        password = request.form.get("password") or ""
        posted_next = _safe_redirect_target(request.form.get("next"))
        if posted_next:
            next_url = posted_next
        if not username or not password:
            error = "Kullanici adi ve sifre gerekli."
        else:
            account = get_account_by_username(username)
            if not account or not check_password_hash(account["password_hash"], password):
                error = "Gecersiz giris bilgileri."
            else:
                session.clear()
                session["account_id"] = int(account["id"])
                session["username"] = account["username"]
                session["unit_id"] = int(account["unit_id"])
                return redirect(next_url or url_for("planla"))
    return render_template("login.html", error=error, next_url=next_url)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/set-language/<lang>")
def set_language(lang: str):
    if lang not in SUPPORTED_LANGUAGES:
        lang = "tr"
    session["language"] = lang
    next_hint = _safe_redirect_target(request.args.get("next"))
    if next_hint:
        return redirect(next_hint)
    if "unit_id" in session:
        return redirect(url_for("planla"))
    return redirect(url_for("login"))


def compute_plan(unit_id: int, year=None, month=None, plan_type: str = "clinic", *, clinics=None, duty_types=None):
    fallback_notes: List[str] = []
    today = date.today()
    selected_year = _safe_int(year) or today.year
    selected_month = _safe_int(month) or today.month

    normalized_plan = (plan_type or "clinic").strip().lower()
    if normalized_plan not in {"clinic", "nobet"}:
        normalized_plan = "clinic"

    staff_rows_raw = list(list_staff(unit_id))
    if not staff_rows_raw:
        error = _("Lütfen önce personel ekleyin. /personel sayfasından kayıt oluşturabilirsiniz.")
        return None, error, 400

    staff_records = [dict(row) for row in staff_rows_raw]
    staff_name_map = {row["id"]: row.get("name") for row in staff_records}

    clinic_rows_source = clinics if clinics is not None else list(list_clinics(unit_id))
    valid_rotation_values = {option[0] for option in CLINIC_ROTATION_OPTIONS}
    clinic_records = []
    for row in clinic_rows_source:
        row_dict = dict(row)
        responsible_id = row_dict.get("sorumlu_uzman_id")
        row_dict["responsible_name"] = (
            staff_name_map.get(responsible_id) if responsible_id is not None else None
        )
        rotation_value = (row_dict.get("rotation_period") or DEFAULT_ROTATION_PERIOD).strip().lower()
        if rotation_value not in valid_rotation_values:
            rotation_value = DEFAULT_ROTATION_PERIOD
        row_dict["rotation_period"] = rotation_value
        row_dict["rotation_period_label"] = CLINIC_ROTATION_LABELS.get(rotation_value, rotation_value.title())
        clinic_records.append(row_dict)

    clinic_rotation_periods: Dict[int, str] = {}
    for clinic in clinic_records:
        clinic_id = clinic.get("id")
        try:
            clinic_id_int = int(clinic_id)
        except (TypeError, ValueError):
            continue
        clinic_rotation_periods[clinic_id_int] = clinic.get("rotation_period", DEFAULT_ROTATION_PERIOD)

    clinic_rule_rows = [dict(row) for row in list(list_clinic_seniority_rules(unit_id))]
    clinic_rule_map: Dict[int, Dict[str, int]] = defaultdict(dict)
    for rule in clinic_rule_rows:
        clinic_id_raw = rule.get("clinic_id")
        try:
            clinic_id_int = int(clinic_id_raw)
        except (TypeError, ValueError):
            continue
        seniority_key = (rule.get("required_seniority") or "").strip().lower()
        if not seniority_key:
            continue
        try:
            count_value = int(rule.get("required_count", 0))
        except (TypeError, ValueError):
            continue
        clinic_rule_map[clinic_id_int][seniority_key] = count_value
    clinic_repeat_history: Dict[int, Set[str]] = defaultdict(set)
    if normalized_plan != "nobet":
        previous_year, previous_month = _previous_month(selected_year, selected_month)
        previous_period = _plan_period(previous_year, previous_month)
        history_rows = [dict(row) for row in list(list_assignment_history(unit_id, previous_period))]
        for history in history_rows:
            clinic_id_raw = history.get("clinic_id")
            staff_id_raw = history.get("staff_id")
            try:
                clinic_id_int = int(clinic_id_raw)
                staff_id_int = int(staff_id_raw)
            except (TypeError, ValueError):
                continue
            clinic_repeat_history[clinic_id_int].add(f"staff_{staff_id_int}")
    clinic_repeat_payload: Dict[int, List[str]] = {
        clinic_id: sorted(list(people))
        for clinic_id, people in clinic_repeat_history.items()
        if people
    }
    weekend_history_counts: Dict[str, int] = defaultdict(int)
    if normalized_plan == "nobet":
        history_year = selected_year
        history_month = selected_month
        for _month_offset in range(WEEKEND_HISTORY_MONTHS):
            history_year, history_month = _previous_month(history_year, history_month)
            if history_year < 1:
                break
            period = _plan_period(history_year, history_month)
            for history in list_assignment_history(unit_id, period):
                day_type = (history.get("day_type") or "").strip().lower()
                if day_type != "weekend":
                    continue
                staff_id_raw = history.get("staff_id")
                try:
                    staff_id_int = int(staff_id_raw)
                except (TypeError, ValueError):
                    continue
                weekend_history_counts[f"staff_{staff_id_int}"] += 1

    weekend_history_counts = dict(weekend_history_counts)

    leave_rows = [dict(row) for row in list(list_leave_requests(unit_id))]
    leave_requests_map: Dict[int, List[tuple[date, date]]] = defaultdict(list)
    for leave in leave_rows:
        staff_id_raw = leave.get("staff_id")
        try:
            staff_id_int = int(staff_id_raw)
        except (TypeError, ValueError):
            continue
        start_raw = (leave.get("start_date") or "").strip()
        end_raw = (leave.get("end_date") or "").strip()
        try:
            start_dt = date.fromisoformat(start_raw)
            end_dt = date.fromisoformat(end_raw)
        except ValueError:
            continue
        if end_dt < start_dt:
            start_dt, end_dt = end_dt, start_dt
        leave_requests_map[staff_id_int].append((start_dt, end_dt))

    duty_rows_source = duty_types if duty_types is not None else list(list_duty_types(unit_id))
    duty_type_records = [dict(row) for row in duty_rows_source]

    people = people_from_records(staff_records)

    if normalized_plan == "nobet":
        nobet_duty_types = [
            row for row in duty_type_records
            if (row.get("duty_category") or "nobet").strip().lower() == "nobet"
        ]

        cap_definition = next(
            (row for row in nobet_duty_types if (row.get("name") or "").strip().lower() == "cap"),
            None,
        )
        if cap_definition is None:
            error = _("İcap nöbeti tanımı bulunamadı. /nöbetler sayfasından ekleyin.")
            return None, error, 400

        night_duties = [row for row in nobet_duty_types if row is not cap_definition]

        try:
            cap_result = build_cap_plan(
                people=people,
                cap_duty=cap_definition,
                year=selected_year,
                month=selected_month,
                leave_requests=leave_requests_map,
            )
        except ValueError as exc:
            return None, _(str(exc)), 400
        try:
            night_result = build_night_plan(
                people=people,
                night_duties=night_duties,
                year=selected_year,
                month=selected_month,
                leave_requests=leave_requests_map,
                weekend_history=weekend_history_counts,
            )
        except ValueError as exc:
            return None, _(str(exc)), 400

        combined_assignments = cap_result["assignments"] + night_result["assignments"]
        combined_assignments.sort(key=lambda item: item.get("start") or "")

        result = {
            "status_label": night_result.get("status_label") or cap_result.get("status_label") or "OK",
            "status_code": None,
            "objective_value": night_result.get("objective_value", 0),
            "assignments": combined_assignments,
            "loads": {
                "cap": cap_result.get("loads", []),
                "night": night_result.get("loads", []),
            },
            "text": "\n\n".join([section for section in [cap_result.get("text"), night_result.get("text")] if section]),
            "cap_summary": cap_result.get("cap_summary", []),
            "night_summary": night_result.get("summary_rows", []),
        }
        fallback_notes.extend(night_result.get("fallback_notes", []))
    else:
        mesa_duty_types = [
            row for row in duty_type_records
            if (row.get("duty_category") or "nobet").strip().lower() == "mesa"
        ]

        if not clinic_records and not mesa_duty_types:
            error = _(
                "Planlama için en az bir klinik veya mesai görevi ekleyin. /klinikler ve /nöbetler sayfalarını kullanabilirsiniz."
            )
            return None, error, 400

        slots = slots_from_records(
            clinics=clinic_records,
            duty_types=mesa_duty_types,
            year=selected_year,
            month=selected_month,
            plan_type=normalized_plan,
        )
        if not slots:
            error = _("Bu verilerle oluşturulacak slot bulunamadı. Klinik ve görev tanımlarınızı kontrol edin.")
            return None, error, 400

        clinic_rules_map = {cid: dict(rules) for cid, rules in clinic_rule_map.items()}
        try:
            result = solve_schedule(
                people=people,
                slots=slots,
                clinic_rotation_periods=clinic_rotation_periods,
                clinic_seniority_rules=clinic_rules_map,
                clinic_repeat_history=clinic_repeat_payload,
                staff_leave_requests=leave_requests_map,
            )
        except RuntimeError as exc:
            try:
                result = solve_schedule(
                    people=people,
                    slots=slots,
                    clinic_rotation_periods=clinic_rotation_periods,
                    clinic_seniority_rules=clinic_rules_map,
                    clinic_repeat_history=None,
                    staff_leave_requests=leave_requests_map,
                )
                fallback_notes.append(
                    _("Çözüm bulunamadığı için tekrar cezası devre dışı bırakıldı; ardışık klinik atamaları oluşabilir.")
                )
            except RuntimeError:
                try:
                    result = solve_schedule(
                        people=people,
                        slots=slots,
                        clinic_rotation_periods=clinic_rotation_periods,
                        clinic_seniority_rules=None,
                        clinic_repeat_history=None,
                        staff_leave_requests=leave_requests_map,
                    )
                    fallback_notes.append(
                        _("Kıdem gereksinimleri gevşetildi; klinik kadrosunu manuel olarak gözden geçirin.")
                    )
                except RuntimeError as exc_final:
                    error = _("Planlama sırasında hata oluştu: {detay}", detay=exc_final)
                    return None, error, 500
        except Exception as exc:  # pragma: no cover - safeguarding prototype
            error = _("Planlama sırasında hata oluştu: {detay}", detay=exc)
            return None, error, 500

    result["selected_year"] = selected_year
    result["selected_month"] = selected_month
    result["plan_type"] = normalized_plan
    result["plan_period"] = _plan_period(selected_year, selected_month)
    result["fallback_notes"] = fallback_notes
    return result, None, None


def build_cap_plan(*, people, cap_duty, year, month, leave_requests=None):
    specialists = [
        person
        for person in people
        if (person.title or "").strip().lower().startswith("uzm")
    ]
    if not specialists:
        raise ValueError(_("İcap nöbeti için en az bir 'Uzm. Dr.' gereklidir."))

    specialists.sort(key=lambda p: (p.display_name or "").lower())

    first_weekday, days_in_month = calendar.monthrange(year, month)
    first_day = date(year, month, 1)
    days = [first_day + timedelta(days=offset) for offset in range(days_in_month)]

    duty_name_raw = (cap_duty.get("name") or "cap").strip() or "cap"
    duty_id = cap_duty.get("id")
    duty_name_display = _( "İcap") if duty_name_raw.lower() == "cap" else duty_name_raw

    leave_map = leave_requests or {}
    specialist_leave_windows: Dict[str, List[tuple[date, date]]] = {}
    for specialist in specialists:
        identifier = specialist.identifier
        staff_id = None
        if identifier.startswith("staff_"):
            try:
                staff_id = int(identifier.split("_", 1)[1])
            except ValueError:
                staff_id = None
        if staff_id is None:
            continue
        windows = leave_map.get(staff_id)
        if windows:
            specialist_leave_windows[identifier] = list(windows)

    def is_on_leave(person: "Person", day: date) -> bool:
        windows = specialist_leave_windows.get(person.identifier)
        if not windows:
            return False
        for start_date, end_date in windows:
            if start_date <= day <= end_date:
                return True
        return False

    loads = {}
    for specialist in specialists:
        loads[specialist.identifier] = {
            "person_id": specialist.identifier,
            "person_name": specialist.display_name,
            "title": specialist.title,
            "seniority": specialist.seniority,
            "assigned_days": 0,
            "weekday_days": 0,
            "weekend_days": 0,
            "total_hours": 0,
        }

    assignments = []
    num_specialists = len(specialists)
    pointer = 0

    for day in days:
        assigned_specialist = None
        for offset in range(num_specialists):
            candidate = specialists[(pointer + offset) % num_specialists]
            if is_on_leave(candidate, day):
                continue
            assigned_specialist = candidate
            pointer = (pointer + offset + 1) % num_specialists
            break
        if assigned_specialist is None:
            raise ValueError(
                _(
                    "{date} tarihindeki icap nöbeti için tüm uzmanlar izinli. İzinleri düzenleyin.",
                    date=day.isoformat(),
                )
            )

        is_weekend = day.weekday() >= 5
        hours = 24 if is_weekend else 16
        start_dt = datetime.combine(day, datetime.min.time())

        assignments.append(
            {
                "slot_id": f"duty_{duty_id}_{day.isoformat()}" if duty_id is not None else f"cap_{day.isoformat()}",
                "duty_type": duty_name_display,
                "label": f"{duty_name_display} - {day.isoformat()}",
                "start": start_dt.isoformat(),
                "duration_hours": hours,
                "requires_extended_rest": False,
                "person_id": assigned_specialist.identifier,
                "person_name": assigned_specialist.display_name,
                "person_title": assigned_specialist.title,
                "person_seniority": assigned_specialist.seniority,
            }
        )

        load_entry = loads[assigned_specialist.identifier]
        load_entry["assigned_days"] += 1
        if is_weekend:
            load_entry["weekend_days"] += 1
        else:
            load_entry["weekday_days"] += 1
        load_entry["total_hours"] += hours

    loads_list = sorted(loads.values(), key=lambda entry: entry["person_name"].lower())
    summary_rows = [
        {
            "Personel": entry["person_name"],
            "Unvan": entry["title"] or "-",
            "Atanan Gün": entry["assigned_days"],
            "Hafta İçi Gün": entry["weekday_days"],
            "Hafta Sonu Gün": entry["weekend_days"],
            "Toplam Saat": entry["total_hours"],
        }
        for entry in loads_list
        if entry["assigned_days"] > 0
    ]

    lines = [
        _("=== İcap Nöbet Planı ==="),
        _("Uzman sayısı: {count}", count=num_specialists),
        _("Toplam gün: {count}", count=len(days)),
        "",
        _("Dağılım:"),
    ]
    if summary_rows:
        for row in summary_rows:
            lines.append(
                _(
                    "- {person}: {assignments} gün (hafta içi {weekday}, hafta sonu {weekend}), toplam {hours} saat",
                    person=row["Personel"],
                    assignments=row["Atanan Gün"],
                    weekday=row["Hafta İçi Gün"],
                    weekend=row["Hafta Sonu Gün"],
                    hours=row["Toplam Saat"],
                )
            )
    else:
        lines.append(_("- Uzmanlara görev atanmadı."))

    text = "\n".join(lines)

    result = {
        "status_label": "OK",
        "status_code": None,
        "objective_value": 0,
        "assignments": assignments,
        "loads": loads_list,
        "text": text,
        "cap_summary": summary_rows,
        "plan_type": "nobet",
    }
    return result


def build_night_plan(*, people, night_duties, year, month, leave_requests=None, weekend_history=None):
    if not night_duties:
        return {
            "assignments": [],
            "loads": [],
            "text": _("Gece nöbeti tanımı bulunmuyor."),
            "summary_rows": [],
            "status_label": "EMPTY",
            "objective_value": 0,
        }

    assistant_people = [
        person
        for person in people
        if (person.title or "").strip().lower().startswith("asst")
    ]
    for assistant in assistant_people:
        if (
            assistant.min_night_duties is not None
            and assistant.max_night_duties is not None
            and assistant.min_night_duties > assistant.max_night_duties
        ):
            raise ValueError(
                _(
                    "Geçerli olmayan nöbet sınırları: {name} için minimum {minimum} maksimumdan büyük.",
                    name=assistant.display_name,
                    minimum=assistant.min_night_duties,
                )
            )
    if not assistant_people:
        raise ValueError(_("Gece nöbeti için en az bir 'Asst. Dr.' gereklidir."))

    slots = slots_from_records(
        clinics=[],
        duty_types=night_duties,
        year=year,
        month=month,
        plan_type="nobet",
    )
    if not slots:
        return {
            "assignments": [],
            "loads": [],
            "text": _("Gece nöbeti slotu oluşmadı."),
            "summary_rows": [],
            "status_label": "EMPTY",
            "objective_value": 0,
        }

    weekend_history_map = dict(weekend_history or {})
    used_relaxed_weekend = False
    try:
        solver_result = solve_schedule(
            assistant_people,
            slots,
            enforce_person_limits=True,
            clinic_repeat_history=None,
            staff_leave_requests=leave_requests,
            weekend_history_counts=weekend_history_map,
            objective_mode="balanced",
        )
    except RuntimeError:
        used_relaxed_weekend = True
        try:
            solver_result = solve_schedule(
                assistant_people,
                slots,
                enforce_person_limits=True,
                clinic_repeat_history=None,
                staff_leave_requests=leave_requests,
                weekend_history_counts=None,
                objective_mode="balanced",
            )
        except RuntimeError as exc:
            raise ValueError(_("Gece nöbeti atamaları için çözüm bulunamadı: {detay}", detay=exc)) from exc
    assignments = solver_result["assignments"]

    summary = {}
    for assignment in assignments:
        person_id = assignment.get("person_id")
        if not person_id:
            continue
        start_iso = assignment.get("start")
        try:
            start_dt = datetime.fromisoformat(start_iso) if start_iso else None
        except ValueError:
            start_dt = None
        is_weekend = start_dt.date().weekday() >= 5 if start_dt else False
        entry = summary.setdefault(
            person_id,
            {
                "assigned_slots": 0,
                "weekday_slots": 0,
                "weekend_slots": 0,
                "total_hours": 0,
            },
        )
        entry["assigned_slots"] += 1
        if is_weekend:
            entry["weekend_slots"] += 1
        else:
            entry["weekday_slots"] += 1
        hours = assignment.get("duration_hours") or 0
        try:
            entry["total_hours"] += int(hours)
        except (TypeError, ValueError):
            pass

    solver_load_map = {
        load["person_id"]: load for load in solver_result.get("loads", [])
    }

    loads_enriched = []
    for person in assistant_people:
        person_id = person.identifier
        summary_entry = summary.get(person_id, {
            "assigned_slots": 0,
            "weekday_slots": 0,
            "weekend_slots": 0,
            "total_hours": 0,
        })
        solver_load = solver_load_map.get(person_id, {})
        loads_enriched.append(
            {
                "person_id": person_id,
                "person_name": person.display_name,
                "title": person.title,
                "seniority": person.seniority,
                "assigned_slots": summary_entry["assigned_slots"],
                "weekday_slots": summary_entry["weekday_slots"],
                "weekend_slots": summary_entry["weekend_slots"],
                "total_hours": summary_entry["total_hours"],
                "target_slots": solver_load.get("target_slots"),
                "deviation": solver_load.get("deviation"),
                "solver_assigned_hours": solver_load.get("assigned_hours"),
                "solver_weekend_slots": solver_load.get("weekend_assigned"),
                "history_weekend_slots": solver_load.get("weekend_history"),
                "min_limit": person.min_night_duties,
                "max_limit": person.max_night_duties,
            }
        )

    summary_rows = [
        {
            "Personel": entry["person_name"],
            "Unvan": entry["title"] or "-",
            "Atanan Görev": entry["assigned_slots"],
            "Hafta İçi": entry["weekday_slots"],
            "Hafta Sonu": entry["weekend_slots"],
            "Toplam Saat": entry["total_hours"],
            "Minimum Limit": entry["min_limit"] if entry["min_limit"] is not None else "-",
            "Maksimum Limit": entry["max_limit"] if entry["max_limit"] is not None else "-",
        }
        for entry in loads_enriched
        if entry["assigned_slots"] > 0
    ]

    lines = [
        _("=== Gece Nöbeti Planı ==="),
        _("Asistan sayısı: {count}", count=len(assistant_people)),
        _("Toplam görev: {count}", count=len(assignments)),
        "",
        _("Dağılım:"),
    ]
    if summary_rows:
        for row in summary_rows:
            lines.append(
                _(
                    "- {person}: {assignments} görev (hafta içi {weekday}, hafta sonu {weekend}), toplam {hours} saat",
                    person=row["Personel"],
                    assignments=row["Atanan Görev"],
                    weekday=row["Hafta İçi"],
                    weekend=row["Hafta Sonu"],
                    hours=row["Toplam Saat"],
                )
            )
    else:
        lines.append(_("- Asistanlara görev atanmadı."))

    result_dict = {
        "assignments": assignments,
        "loads": loads_enriched,
        "text": "\n".join(lines),
        "summary_rows": summary_rows,
        "status_label": solver_result.get("status_label"),
        "objective_value": solver_result.get("objective_value", 0),
    }
    if used_relaxed_weekend:
        result_dict["fallback_notes"] = [
            _(
                "Hafta sonu denge geçmişi gevşetildi; hafta sonu planlamasını manuel olarak doğrulayın."
            ),
        ]
    else:
        result_dict["fallback_notes"] = []
    return result_dict


def build_year_options(selected_year):
    current_year = date.today().year
    start_year = max(1, current_year - DEFAULT_YEAR_SPAN)
    end_year = current_year + DEFAULT_YEAR_SPAN
    options = list(range(start_year, end_year + 1))
    if selected_year not in options:
        options.append(selected_year)
        options.sort()
    return options


def build_plan_table(assignments, clinics, duty_types, year, month, plan_type: str):
    first_weekday, days_in_month = calendar.monthrange(year, month)
    first_day = date(year, month, 1)
    days = [first_day + timedelta(days=offset) for offset in range(days_in_month)]

    normalized_plan = (plan_type or "clinic").strip().lower()

    clinic_dicts = []
    for row in clinics or []:
        row_dict = dict(row)
        row_dict["id"] = _safe_int(row_dict.get("id"))
        row_dict["display_order"] = _safe_int(row_dict.get("display_order"))
        clinic_dicts.append(row_dict)

    def clinic_sort_key(item):
        order_val = item.get("display_order")
        identifier = item.get("id")
        fallback = identifier if identifier is not None else 0
        return (order_val is None, order_val if order_val is not None else fallback, fallback)

    sorted_clinics = sorted(clinic_dicts, key=clinic_sort_key)

    duty_dicts = []
    for row in duty_types or []:
        row_dict = dict(row)
        row_dict["id"] = _safe_int(row_dict.get("id"))
        row_dict["duty_category"] = (row_dict.get("duty_category") or "").strip().lower()
        duty_dicts.append(row_dict)
    duty_dicts.sort(key=lambda item: item.get("id") if item.get("id") is not None else 0)

    include_clinic_columns = normalized_plan != "nobet"
    include_duty_columns = normalized_plan != "clinic"

    assignment_duty_ids = set()
    if include_duty_columns:
        for assignment in assignments or []:
            slot_id = assignment.get("slot_id") or ""
            if slot_id.startswith("duty_"):
                parts = slot_id.split("_", 2)
                try:
                    assigned_id = int(parts[1])
                except (IndexError, ValueError):
                    continue
                assignment_duty_ids.add(assigned_id)

    columns = [("Tarih", ("date", None))]
    if include_clinic_columns:
        for clinic in sorted_clinics:
            clinic_id = clinic.get("id")
            if clinic_id is None:
                continue
            header = clinic.get("name") or f"Klinik {clinic_id}"
            responsible = clinic.get("responsible_name")
            if responsible:
                header = f"{header} ({_('Sorumlu: {name}', name=responsible)})"
            columns.append((header, ("clinic", clinic_id)))
    if include_duty_columns:
        for duty in duty_dicts:
            duty_id = duty.get("id")
            if duty_id is None:
                continue
            category = duty.get("duty_category")
            if normalized_plan == "clinic" and category != "mesa":
                continue
            if normalized_plan == "nobet" and category != "nobet":
                continue
            if normalized_plan == "nobet" and assignment_duty_ids and duty_id not in assignment_duty_ids:
                continue
            header = duty.get("name") or f"Gorev {duty_id}"
            columns.append((header, ("duty", duty_id)))

    lookup = defaultdict(list)
    for assignment in assignments or []:
        start_iso = assignment.get("start")
        try:
            start_dt = datetime.fromisoformat(start_iso) if start_iso else None
        except ValueError:
            start_dt = None
        if start_dt is None:
            continue
        day = start_dt.date()
        slot_id = assignment.get("slot_id") or ""
        key = None
        if slot_id.startswith("clinic_"):
            parts = slot_id.split("_", 2)
            try:
                slot_clinic_id = int(parts[1])
            except (IndexError, ValueError):
                slot_clinic_id = None
            if slot_clinic_id is not None:
                key = ("clinic", slot_clinic_id)
        elif slot_id.startswith("duty_"):
            parts = slot_id.split("_", 2)
            try:
                slot_duty_id = int(parts[1])
            except (IndexError, ValueError):
                slot_duty_id = None
            if slot_duty_id is not None and include_duty_columns:
                if normalized_plan == "nobet" and assignment_duty_ids and slot_duty_id not in assignment_duty_ids:
                    continue
                key = ("duty", slot_duty_id)
        if key is None:
            continue
        person_name = assignment.get("person_name") or _("Atanmamış")
        person_title = assignment.get("person_title")
        display_label = f"{person_title} {person_name}".strip() if person_title else person_name
        lookup[(day, key)].append(display_label)

    headers = [header for header, _ in columns]
    rows = []
    for day in days:
        row = {"Tarih": day.isoformat()}
        for header, key in columns[1:]:
            values = lookup.get((day, key), [])
            row[header] = ", ".join(values) if values else ""
        rows.append(row)

    return {"headers": headers, "rows": rows}


@app.route("/planla", methods=["GET"])
@login_required
def planla():
    unit_id = _require_unit_id()
    today = date.today()
    requested_year = request.args.get("year", type=int)
    requested_month = request.args.get("month", type=int)
    selected_year = requested_year or today.year
    selected_month = requested_month or today.month
    approval_message = request.args.get("approval_message")
    approval_error = request.args.get("approval_error")

    requested_plan_type = (request.args.get("plan_type") or "clinic").strip().lower()
    selected_plan_type = requested_plan_type if requested_plan_type in {option[0] for option in PLAN_TYPE_OPTIONS} else "clinic"

    staff_rows_for_plan = [dict(row) for row in list(list_staff(unit_id))]
    staff_name_map_for_plan = {row["id"]: row.get("name") for row in staff_rows_for_plan}

    clinic_records = []
    for row in list(list_clinics(unit_id)):
        row_dict = dict(row)
        responsible_id = row_dict.get("sorumlu_uzman_id")
        row_dict["responsible_name"] = (
            staff_name_map_for_plan.get(responsible_id) if responsible_id is not None else None
        )
        clinic_records.append(row_dict)

    duty_type_records = [dict(row) for row in list(list_duty_types(unit_id))]

    result, error_message, error_status = compute_plan(
        unit_id=unit_id,
        year=selected_year,
        month=selected_month,
        plan_type=selected_plan_type,
        clinics=clinic_records,
        duty_types=duty_type_records,
    )

    plan_table = None
    if result:
        plan_table = build_plan_table(
            assignments=result.get("assignments"),
            clinics=clinic_records,
            duty_types=duty_type_records,
            year=selected_year,
            month=selected_month,
            plan_type=selected_plan_type,
        )

    year_options = build_year_options(selected_year)
    download_url = url_for(
        "download_plan",
        year=selected_year,
        month=selected_month,
        plan_type=selected_plan_type,
    )
    month_label = next((label for value, label in MONTH_OPTIONS if value == selected_month), str(selected_month))
    plan_type_labels = {value: label for value, label in PLAN_TYPE_OPTIONS}
    plan_type_label = plan_type_labels.get(selected_plan_type, "Klinik Mesai Planı Oluştur")

    return render_template(
        "planla.html",
        error=error_message,
        error_status=error_status,
        result=result,
        plan_table=plan_table,
        year_options=year_options,
        month_options=MONTH_OPTIONS,
        selected_year=selected_year,
        selected_month=selected_month,
        download_url=download_url,
        selected_month_label=month_label,
        plan_type_options=PLAN_TYPE_OPTIONS,
        selected_plan_type=selected_plan_type,
        selected_plan_type_label=plan_type_label,
        approval_message=approval_message,
        approval_error=approval_error,
        plan_period=_plan_period(selected_year, selected_month),
    )


@app.route("/planla/approve", methods=["POST"])
@login_required
def planla_approve():
    unit_id = _require_unit_id()
    year_raw = request.form.get("year")
    month_raw = request.form.get("month")
    plan_type_raw = (request.form.get("plan_type") or "clinic").strip().lower()
    allowed_plan_types = {value for value, _ in PLAN_TYPE_OPTIONS}
    if plan_type_raw not in allowed_plan_types:
        plan_type_raw = "clinic"
    year = _safe_int(year_raw)
    month = _safe_int(month_raw)
    if year is None or month is None:
        return redirect(
            url_for(
                "planla",
                approval_error=_("Geçerli bir yıl ve ay seçin."),
                plan_type=plan_type_raw,
            )
        )

    clinic_records = [dict(row) for row in list(list_clinics(unit_id))]
    duty_type_records = [dict(row) for row in list(list_duty_types(unit_id))]
    result, error_message, _error_status = compute_plan(
        unit_id=unit_id,
        year=year,
        month=month,
        plan_type=plan_type_raw,
        clinics=clinic_records,
        duty_types=duty_type_records,
    )
    if error_message or not result:
        return redirect(
            url_for(
                "planla",
                year=year,
                month=month,
                plan_type=plan_type_raw,
                 approval_error=error_message or _("Plan oluşturulamadı."),
            )
        )

    assignments = result.get("assignments") or []
    new_entries: List[Tuple[int, Optional[int], str, str]] = []
    store_clinic = plan_type_raw == "clinic"
    store_night = plan_type_raw == "nobet"
    for assignment in assignments:
        slot_id = assignment.get("slot_id") or ""
        if store_clinic:
            if not slot_id.startswith("clinic_"):
                continue
            clinic_id = _extract_clinic_id(slot_id)
            if clinic_id is None:
                continue
        elif store_night:
            if not slot_id.startswith("duty_"):
                continue
            clinic_id = None
        else:
            continue
        person_identifier = assignment.get("person_id") or ""
        if not person_identifier.startswith("staff_"):
            continue
        try:
            staff_id = int(person_identifier.split("_", 1)[1])
        except ValueError:
            continue
        start_iso = assignment.get("start")
        try:
            assignment_date_obj = datetime.fromisoformat(start_iso).date()
        except (TypeError, ValueError):
            continue
        day_type = _classify_day_type(assignment_date_obj)
        new_entries.append((staff_id, clinic_id, assignment_date_obj.isoformat(), day_type))

    plan_period = _plan_period(year, month)
    existing_rows = list(list_assignment_history(unit_id, plan_period))
    preserved_entries: List[Tuple[int, Optional[int], str, str]] = []
    for record in existing_rows:
        staff_id = _safe_int(record.get("staff_id"))
        assignment_date = record.get("assignment_date")
        if staff_id is None or not assignment_date:
            continue
        clinic_id_existing = record.get("clinic_id")
        day_type_existing = (record.get("day_type") or "weekday").strip().lower()
        if day_type_existing not in {"weekday", "weekend"}:
            day_type_existing = "weekday"
        entry_tuple = (staff_id, clinic_id_existing, assignment_date, day_type_existing)
        if store_clinic:
            if clinic_id_existing is None:
                preserved_entries.append(entry_tuple)
        elif store_night:
            if clinic_id_existing is not None:
                preserved_entries.append(entry_tuple)
        else:
            preserved_entries.append(entry_tuple)

    combined_entries = preserved_entries + new_entries
    if combined_entries or store_clinic or store_night:
        replace_assignment_history(unit_id, plan_period, combined_entries)

    return redirect(
        url_for(
            "planla",
            year=year,
            month=month,
            plan_type=plan_type_raw,
            approval_message=_("Plan kaydedildi."),
        )
    )


@app.route("/download-plan", methods=["GET"])
@login_required
def download_plan():
    unit_id = _require_unit_id()
    if not PANDAS_AVAILABLE:
        body = _(
            "Excel çıktısı için pandas ve openpyxl kütüphaneleri gerekli.\nKurulum: pip install pandas openpyxl"
        )
        return body, 500, {"Content-Type": "text/plain; charset=utf-8"}

    today = date.today()
    requested_year = request.args.get("year", type=int)
    requested_month = request.args.get("month", type=int)
    selected_year = requested_year or today.year
    selected_month = requested_month or today.month
    requested_plan_type = (request.args.get("plan_type") or "clinic").strip().lower()
    selected_plan_type = requested_plan_type if requested_plan_type in {option[0] for option in PLAN_TYPE_OPTIONS} else "clinic"

    staff_rows_for_download = [dict(row) for row in list(list_staff(unit_id))]
    staff_name_map_for_download = {row["id"]: row.get("name") for row in staff_rows_for_download}

    clinic_records = []
    for row in list(list_clinics(unit_id)):
        row_dict = dict(row)
        responsible_id = row_dict.get("sorumlu_uzman_id")
        row_dict["responsible_name"] = (
            staff_name_map_for_download.get(responsible_id) if responsible_id is not None else None
        )
        clinic_records.append(row_dict)

    duty_type_records = [dict(row) for row in list(list_duty_types(unit_id))]
    result, error_message, error_status = compute_plan(
        unit_id=unit_id,
        year=selected_year,
        month=selected_month,
        plan_type=selected_plan_type,
        clinics=clinic_records,
        duty_types=duty_type_records,
    )
    if error_message:
        status_code = error_status or 400
        return error_message, status_code, {"Content-Type": "text/plain; charset=utf-8"}

    plan_table = build_plan_table(
        assignments=result.get("assignments"),
        clinics=clinic_records,
        duty_types=duty_type_records,
        year=selected_year,
        month=selected_month,
        plan_type=selected_plan_type,
    )

    df = pd.DataFrame(plan_table["rows"])
    df = df.reindex(columns=plan_table["headers"])

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Plan")
        if selected_plan_type == "nobet":
            summary_rows = result.get("cap_summary") or []
            if summary_rows:
                summary_df = pd.DataFrame(summary_rows)
                summary_df.to_excel(writer, index=False, sheet_name=_("İcap Özeti"))
            night_rows = result.get("night_summary") or []
            if night_rows:
                night_df = pd.DataFrame(night_rows)
                night_df.to_excel(writer, index=False, sheet_name=_("Gece Nöbeti Özeti"))
    output.seek(0)

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"plan-{selected_plan_type}-{selected_year}-{selected_month:02d}-{timestamp}.xlsx"
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

@app.route("/personel", methods=["GET", "POST"])
@login_required
def personel():
    error = None
    title_options = ["Uzm. Dr.", "Asst. Dr."]
    seniority_options = SENIORITY_CHOICES
    night_limit_options = list(range(0, 11))
    allowed_seniority_values = {value for value, _label in seniority_options}
    unit_id = _require_unit_id()

    if request.method == "POST":
        action = (request.form.get("action") or "add").strip().lower()
        if action == "delete":
            staff_id = _safe_int(request.form.get("staff_id"))
            if not staff_id:
                error = _("Geçerli bir personel seçin.")
            else:
                delete_staff(staff_id, unit_id)
                return redirect(url_for("personel"))
        elif action == "update":
            staff_id = _safe_int(request.form.get("staff_id"))
            if not staff_id:
                error = _("Geçerli bir personel seçin.")
            else:
                staff_row = get_staff_by_id(staff_id, unit_id)
                if not staff_row:
                    error = _("Personel kaydı bulunamadı.")
                elif (staff_row["title"] or "").strip() != "Asst. Dr.":
                    error = _("Yalnızca Asst. Dr. kayıtları güncellenebilir.")
                else:
                    seniority_raw = (request.form.get("seniority") or "").strip().lower()
                    min_night_raw = (request.form.get("min_night") or "").strip()
                    max_night_raw = (request.form.get("max_night") or "").strip()
                    min_night_value = _safe_int(min_night_raw) if min_night_raw != "" else None
                    max_night_value = _safe_int(max_night_raw) if max_night_raw != "" else None

                    if seniority_raw not in allowed_seniority_values:
                        error = _("Geçerli kıdem seçin.")
                    elif (
                        min_night_value is not None
                        and max_night_value is not None
                        and min_night_value > max_night_value
                    ):
                        error = _("Minimum nöbet sayısı maksimumdan büyük olamaz.")
                    elif (min_night_value is not None and min_night_value < 0) or (
                        max_night_value is not None and max_night_value < 0
                    ):
                        error = _("Nöbet sınırları negatif olamaz.")
                    else:
                        update_staff_preferences(
                            staff_id,
                            seniority=seniority_raw,
                            min_night=min_night_value,
                            max_night=max_night_value,
                            unit_id=unit_id,
                        )
                        return redirect(url_for("personel"))
        elif action == "add":
            name = (request.form.get("name") or "").strip()
            title = (request.form.get("title") or "").strip()
            seniority_raw = (request.form.get("seniority") or "").strip()
            min_night_raw = (request.form.get("min_night") or "").strip()
            max_night_raw = (request.form.get("max_night") or "").strip()

            min_night_value = _safe_int(min_night_raw) if min_night_raw else None
            max_night_value = _safe_int(max_night_raw) if max_night_raw else None
            seniority_value = None

            if not name:
                error = _("Lütfen ad soyad girin.")
            elif title not in title_options:
                error = _("Geçerli ünvan seçin.")
            elif title == "Asst. Dr.":
                if seniority_raw not in allowed_seniority_values:
                    error = _("Geçerli kıdem seçin.")
                elif (min_night_value is not None and min_night_value < 0) or (
                    max_night_value is not None and max_night_value < 0
                ):
                    error = _("Nöbet sınırları negatif olamaz.")
                elif (
                    min_night_value is not None
                    and max_night_value is not None
                    and min_night_value > max_night_value
                ):
                    error = _("Minimum nöbet sayısı maksimumdan büyük olamaz.")
                else:
                    seniority_value = seniority_raw
            else:
                if min_night_value is not None or max_night_value is not None:
                    error = _("Nöbet sınırları yalnızca Asst. Dr. için girilebilir.")
                seniority_value = None
                min_night_value = None
                max_night_value = None

            if error is None:
                add_staff(
                    name=name,
                    title=title,
                    seniority=seniority_value,
                    min_night=min_night_value if title == "Asst. Dr." else None,
                    max_night=max_night_value if title == "Asst. Dr." else None,
                    unit_id=unit_id,
                )
                return redirect(url_for("personel"))
        else:
            error = _("Bilinmeyen işlem tipi.")

    staff_records = list(list_staff(unit_id))
    return render_template(
        "personel.html",
        staff=staff_records,
        error=error,
        seniority_options=seniority_options,
        title_options=title_options,
        seniority_labels=dict(seniority_options),
        night_limit_options=night_limit_options,
    )


@app.route("/izinler", methods=["GET", "POST"])
@login_required
def izinler():
    error = None
    unit_id = _require_unit_id()
    staff_rows = [dict(row) for row in list(list_staff(unit_id))]
    staff_map = {row["id"]: row.get("name") for row in staff_rows}
    form_defaults = {
        "staff_id": "",
        "start_date": "",
        "end_date": "",
        "reason": "",
    }

    if request.method == "POST":
        action = (request.form.get("action") or "add").strip().lower()
        if action == "delete":
            leave_id = _safe_int(request.form.get("leave_id"))
            if not leave_id:
                error = _("Geçerli bir izin kaydı seçin.")
            else:
                delete_leave_request(leave_id, unit_id)
                return redirect(url_for("izinler"))
        elif action == "add":
            staff_id = _safe_int(request.form.get("staff_id"))
            start_date_raw = (request.form.get("start_date") or "").strip()
            end_date_raw = (request.form.get("end_date") or "").strip()
            reason = (request.form.get("reason") or "").strip()

            form_defaults.update(
                {
                    "staff_id": str(staff_id or ""),
                    "start_date": start_date_raw,
                    "end_date": end_date_raw,
                    "reason": reason,
                }
            )

            if not staff_id or staff_id not in staff_map:
                error = _("Geçerli bir personel seçin.")
            elif not start_date_raw or not end_date_raw:
                error = _("Başlangıç ve bitiş tarihlerini girin.")
            else:
                try:
                    start_dt = date.fromisoformat(start_date_raw)
                    end_dt = date.fromisoformat(end_date_raw)
                except ValueError:
                    error = _("Tarih formatları GGGG-AA-GG olmalıdır.")
                else:
                    if end_dt < start_dt:
                        error = _("Bitiş tarihi başlangıçtan önce olamaz.")
                    else:
                        add_leave_request(
                            staff_id=staff_id,
                            start_date=start_dt.isoformat(),
                            end_date=end_dt.isoformat(),
                            reason=reason,
                            unit_id=unit_id,
                        )
                        return redirect(url_for("izinler"))
        else:
            error = _("Bilinmeyen işlem tipi.")

    leave_rows = [dict(row) for row in list(list_leave_requests(unit_id))]
    leave_entries = []
    for leave in leave_rows:
        staff_id = leave.get("staff_id")
        staff_name = staff_map.get(staff_id, f"ID {staff_id}")
        leave_entries.append(
            {
                "id": leave.get("id"),
                "staff_id": staff_id,
                "staff_name": staff_name,
                "start_date": leave.get("start_date"),
                "end_date": leave.get("end_date"),
                "reason": leave.get("reason"),
            }
        )

    return render_template(
        "nler.html",
        staff=staff_rows,
        leaves=leave_entries,
        error=error,
        form_defaults=form_defaults,
    )


@app.route("/nler", methods=["GET", "POST"])
@login_required
def izinler_legacy():
    if request.method == "POST":
        return izinler()
    return redirect(url_for("izinler"))


@app.route("/setup-account")
def setup_account():
    unit_name = "Medeniyet Dermatoloji"
    username = "medeniyet_derma"
    password = "medeniyetderma123"

    init_db()

    units = list_units()
    unit_id = None
    for unit in units:
        if unit.get("name") == unit_name:
            unit_id = int(unit.get("id"))
            break

    if unit_id is None:
        try:
            unit_id = create_unit(unit_name)
        except Exception:
            units = list_units()
            for unit in units:
                if unit.get("name") == unit_name:
                    unit_id = int(unit.get("id"))
                    break
    if unit_id is None:
        return "Ünite oluşturulamadı.", 500

    account = get_account_by_username(username)
    if account is None:
        password_hash = generate_password_hash(password)
        create_unit_account(username, password_hash, unit_id)

    return "Ünite hesabı başarıyla oluşturuldu."

@app.route("/klinikler", methods=["GET", "POST"])
@login_required
def klinikler():
    error = None
    unit_id = _require_unit_id()
    staff_rows = [dict(row) for row in list(list_staff(unit_id))]
    specialists = [
        row
        for row in staff_rows
        if (row.get("title") or "").strip().lower() == "uzm. dr."
    ]
    specialist_ids = {row["id"] for row in specialists}

    if request.method == "POST":
        action = (request.form.get("action") or "add").strip()
        if action == "add":
            name = (request.form.get("name") or "").strip()
            required_raw = request.form.get("required_assistants")
            required_value = _safe_int(required_raw) or 1
            rotation_period = request.form.get("rotation_period") or DEFAULT_ROTATION_PERIOD
            responsible_raw = request.form.get("responsible_specialist")
            responsible_id = _safe_int(responsible_raw) if responsible_raw else None
            if responsible_id not in specialist_ids:
                responsible_id = None
            if required_value < 1:
                required_value = 1
            if not name:
                error = _("Lütfen klinik adını girin.")
            else:
                try:
                    add_clinic(
                        name=name,
                        required_assistants=required_value,
                        sorumlu_uzman_id=responsible_id,
                        rotation_period=rotation_period,
                        unit_id=unit_id,
                    )
                    return redirect(url_for("klinikler"))
                except sqlite3.IntegrityError:
                    error = _("Bu isimde bir klinik zaten mevcut.")
        elif action in {"move_up", "move_down"}:
            clinic_id = _safe_int(request.form.get("clinic_id"))
            if not clinic_id:
                error = _("Geçerli bir klinik seçin.")
            else:
                offset = -1 if action == "move_up" else 1
                moved = reorder_clinic(clinic_id, offset, unit_id=unit_id)
                if moved:
                    return redirect(url_for("klinikler"))
                error = _("Sıralama güncellenemedi.")
        elif action == "update":
            clinic_id = _safe_int(request.form.get("clinic_id"))
            required_raw = request.form.get("required_assistants")
            required_value = _safe_int(required_raw)
            rotation_period = request.form.get("rotation_period") or DEFAULT_ROTATION_PERIOD
            responsible_raw = request.form.get("responsible_specialist")
            responsible_id = _safe_int(responsible_raw) if responsible_raw else None
            if responsible_id not in specialist_ids:
                responsible_id = None
            if not clinic_id:
                error = _("Geçerli bir klinik seçin.")
            elif required_value is None or required_value < 1:
                error = _("Geçerli bir asistan sayısı girin.")
            else:
                update_clinic_required_assistants(
                    clinic_id,
                    required_value,
                    responsible_id,
                    rotation_period=rotation_period,
                    unit_id=unit_id,
                )
                return redirect(url_for("klinikler"))
        elif action == "add_rule":
            clinic_id = _safe_int(request.form.get("clinic_id"))
            seniority_choice = (request.form.get("required_seniority") or "").strip().lower()
            count_raw = request.form.get("required_count")
            count_value = _safe_int(count_raw)
            if not clinic_id:
                error = _("Geçerli bir klinik seçin.")
            elif seniority_choice not in {choice[0] for choice in SENIORITY_CHOICES}:
                error = _("Geçerli bir kıdem seviyesi seçin.")
            elif count_value is None or count_value < 1:
                error = _("Kural adedi 1 veya daha büyük olmalıdır.")
            else:
                try:
                    add_clinic_seniority_rule(clinic_id, seniority_choice, count_value, unit_id=unit_id)
                except ValueError as exc:
                    error = _(str(exc))
                else:
                    return redirect(url_for("klinikler"))
        elif action == "delete_rule":
            rule_id = _safe_int(request.form.get("rule_id"))
            if not rule_id:
                error = _("Geçerli bir kural seçin.")
            else:
                delete_clinic_seniority_rule(rule_id, unit_id)
                return redirect(url_for("klinikler"))
        elif action == "delete":
            clinic_id = _safe_int(request.form.get("clinic_id"))
            if not clinic_id:
                error = _("Geçerli bir klinik seçin.")
            else:
                delete_clinic(clinic_id, unit_id)
                return redirect(url_for("klinikler"))
        else:
            error = _("Bilinmeyen işlem tipi.")

    staff_name_map = {row["id"]: row.get("name") for row in staff_rows}
    rules_lookup = defaultdict(list)
    for rule_row in list(list_clinic_seniority_rules(unit_id)):
        rule_dict = dict(rule_row)
        clinic_id = rule_dict.get("clinic_id")
        if clinic_id is None:
            continue
        seniority_key = (rule_dict.get("required_seniority") or "").strip().lower()
        rule_dict["required_seniority"] = seniority_key
        rule_dict["seniority_label"] = SENIORITY_LABELS.get(seniority_key, seniority_key.title())
        rules_lookup[clinic_id].append(rule_dict)

    clinic_records = []
    for row in list(list_clinics(unit_id)):
        row_dict = dict(row)
        clinic_id = row_dict.get("id")
        rotation_period = (row_dict.get("rotation_period") or DEFAULT_ROTATION_PERIOD).strip().lower()
        if rotation_period not in CLINIC_ROTATION_LABELS:
            rotation_period = DEFAULT_ROTATION_PERIOD
        row_dict["rotation_period"] = rotation_period
        row_dict["rotation_period_label"] = CLINIC_ROTATION_LABELS.get(rotation_period, rotation_period.title())
        responsible_id = row_dict.get("sorumlu_uzman_id")
        row_dict["responsible_name"] = (
            staff_name_map.get(responsible_id) if responsible_id is not None else None
        )
        row_dict["seniority_rules"] = sorted(
            rules_lookup.get(clinic_id, []),
            key=lambda item: item.get("seniority_label", ""),
        )
        clinic_records.append(row_dict)

    return render_template(
        "klinikler.html",
        clinics=clinic_records,
        specialists=specialists,
        error=error,
        rotation_options=CLINIC_ROTATION_OPTIONS,
        seniority_options=SENIORITY_CHOICES,
        default_rotation=DEFAULT_ROTATION_PERIOD,
    )


@app.route("/nobetler", methods=["GET", "POST"])
@login_required
def nobetler():
    error = None
    unit_id = _require_unit_id()
    if request.method == "POST":
        action = (request.form.get("action") or "add").strip().lower()
        if action == "add":
            is_cap = request.form.get("is_cap") == "1"
            if is_cap:
                name = "cap"
                duration = 24
                category = "nobet"
                required_staff = 1
            else:
                name = (request.form.get("name") or "").strip()
                duration_raw = (request.form.get("duration_hours") or "").strip()
                category_raw = (request.form.get("duty_category") or "nobet").strip().lower()
                required_raw = request.form.get("required_staff_count")
                required_value = _safe_int(required_raw) or 1
                if not name or not duration_raw:
                    error = _("Lütfen tüm alanları doldurun.")
                else:
                    try:
                        duration = int(duration_raw)
                    except ValueError:
                        error = _("Süre alanı tam sayı olmalıdır.")
                    else:
                        if duration <= 0:
                            error = _("Süre sıfırdan büyük olmalıdır.")
                        elif required_value < 1:
                            error = _("Geçerli bir personel sayısı girin.")
                        else:
                            category = category_raw if category_raw in {"mesa", "nobet"} else "nobet"
                            required_staff = required_value
            if error is None:
                try:
                    add_duty_type(
                        name=name,
                        duration_hours=duration,
                        duty_category=category,
                        required_staff_count=required_staff,
                        unit_id=unit_id,
                    )
                    return redirect(url_for("nobetler"))
                except sqlite3.IntegrityError:
                    error = _("Bu isimde bir nöbet türü zaten mevcut.")

    duty_types = list(list_duty_types(unit_id))
    return render_template("nobetler.html", duty_types=duty_types, error=error)



if __name__ == "__main__":
    app.run(debug=True)
