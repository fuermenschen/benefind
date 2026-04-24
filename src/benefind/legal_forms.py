"""Official Swiss legal forms from ZEFIX `/api/v1/legalForm`."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class SwissLegalForm(StrEnum):
    """Official German legal form names from ZEFIX."""

    UNBEKANNT = "(unbekannt)"
    EINZELUNTERNEHMEN = "Einzelunternehmen"
    KOLLEKTIVGESELLSCHAFT = "Kollektivgesellschaft"
    AKTIENGESELLSCHAFT = "Aktiengesellschaft"
    GESELLSCHAFT_MIT_BESCHRAENKTER_HAFTUNG = "Gesellschaft mit beschränkter Haftung"
    GENOSSENSCHAFT = "Genossenschaft"
    VEREIN = "Verein"
    STIFTUNG = "Stiftung"
    ZWEIGNIEDERLASSUNG = "Zweigniederlassung"
    KOMMANDITGESELLSCHAFT = "Kommanditgesellschaft"
    ZWEIGNIEDERLASSUNG_EINER_AUSL_GESELLSCHAFT = "Zweigniederlassung einer ausl. Gesellschaft"
    NICHTKAUFMANNISCHE_PROKURA = "Nichtkaufmännische Prokura"
    INSTITUT_DES_OEFFENTLICHEN_RECHTS = "Institut des öffentlichen Rechts"
    KOMMANDITAKTIENGESELLSCHAFT = "Kommanditaktiengesellschaft"
    BESONDERE_RECHTSFORM = "Besondere Rechtsform"
    GEMEINDERSCHAFT = "Gemeinderschaft"
    INVESTMENTGESELLSCHAFT_MIT_FESTEM_KAPITAL = "Investmentgesellschaft mit festem Kapital"
    INVESTMENTGESELLSCHAFT_MIT_VARIABLEM_KAPITAL = "Investmentgesellschaft mit variablem Kapital"
    KOMMANDITGESELLSCHAFT_FUER_KOLLEKTIVE_KAPITALANLAGEN = (
        "Kommanditgesellschaft für kollektive Kapitalanlagen"
    )


@dataclass(frozen=True)
class SwissLegalFormEntry:
    id: int
    uid: str
    name_de: SwissLegalForm
    short_name_de: str


SWISS_LEGAL_FORMS: tuple[SwissLegalFormEntry, ...] = (
    SwissLegalFormEntry(0, "0000", SwissLegalForm.UNBEKANNT, "-"),
    SwissLegalFormEntry(1, "0101", SwissLegalForm.EINZELUNTERNEHMEN, "EIU"),
    SwissLegalFormEntry(2, "0103", SwissLegalForm.KOLLEKTIVGESELLSCHAFT, "KlG"),
    SwissLegalFormEntry(3, "0106", SwissLegalForm.AKTIENGESELLSCHAFT, "AG"),
    SwissLegalFormEntry(
        4,
        "0107",
        SwissLegalForm.GESELLSCHAFT_MIT_BESCHRAENKTER_HAFTUNG,
        "GmbH",
    ),
    SwissLegalFormEntry(5, "0108", SwissLegalForm.GENOSSENSCHAFT, "Gen"),
    SwissLegalFormEntry(6, "0109", SwissLegalForm.VEREIN, "Verein"),
    SwissLegalFormEntry(7, "0110", SwissLegalForm.STIFTUNG, "Stift"),
    SwissLegalFormEntry(9, "0151", SwissLegalForm.ZWEIGNIEDERLASSUNG, "ZN"),
    SwissLegalFormEntry(10, "0104", SwissLegalForm.KOMMANDITGESELLSCHAFT, "KmG"),
    SwissLegalFormEntry(
        11,
        "0111",
        SwissLegalForm.ZWEIGNIEDERLASSUNG_EINER_AUSL_GESELLSCHAFT,
        "ZNA",
    ),
    SwissLegalFormEntry(18, "0118", SwissLegalForm.NICHTKAUFMANNISCHE_PROKURA, "NKP"),
    SwissLegalFormEntry(
        8,
        "0117",
        SwissLegalForm.INSTITUT_DES_OEFFENTLICHEN_RECHTS,
        "IOR",
    ),
    SwissLegalFormEntry(12, "0105", SwissLegalForm.KOMMANDITAKTIENGESELLSCHAFT, "KmAG"),
    SwissLegalFormEntry(13, "0113", SwissLegalForm.BESONDERE_RECHTSFORM, "BES"),
    SwissLegalFormEntry(14, "0119", SwissLegalForm.GEMEINDERSCHAFT, "GEM"),
    SwissLegalFormEntry(
        15,
        "0116",
        SwissLegalForm.INVESTMENTGESELLSCHAFT_MIT_FESTEM_KAPITAL,
        "SICAF",
    ),
    SwissLegalFormEntry(
        16,
        "0115",
        SwissLegalForm.INVESTMENTGESELLSCHAFT_MIT_VARIABLEM_KAPITAL,
        "SICAV",
    ),
    SwissLegalFormEntry(
        17,
        "0114",
        SwissLegalForm.KOMMANDITGESELLSCHAFT_FUER_KOLLEKTIVE_KAPITALANLAGEN,
        "KmGK",
    ),
)


SWISS_LEGAL_FORM_VALUES: tuple[str, ...] = tuple(form.value for form in SwissLegalForm)
SWISS_LEGAL_FORM_UID_BY_NAME: dict[str, str] = {
    str(entry.name_de.value): entry.uid for entry in SWISS_LEGAL_FORMS
}
