import csv
import os
import random
from dataclasses import dataclass
from typing import List, Dict, Tuple

import numpy as np
# --- sklearn baselines ---
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score

# --- datasets (HF) optionnel ---
try:
    from datasets import load_dataset, Dataset, concatenate_datasets

    HF_AVAILABLE = True
except Exception:
    HF_AVAILABLE = False

random.seed(42)
np.random.seed(42)

# =========================
# Configs CuLEMo (papier)
# =========================
COUNTRY_NATIVE_LANG = {
    "USA": "EN",
    "UAE": "AR",
    "DE": "DE",
    "ET": "AM",
    "IN": "HI",
    "MX": "ES",
}
ALL_COUNTRIES = list(COUNTRY_NATIVE_LANG.keys())
ALL_LANGS = ["EN", "AR", "DE", "AM", "HI", "ES"]


# =========================
# Dataset loader
# =========================
@dataclass
class CuRow:
    country: str
    language: str  # prompt language
    text: str  # question / prompt_text
    emotion: str  # gold label in {joy,fear,sadness,anger,guilt,neutral}
    split: str  # train/dev/test


def to_sentiment(e: str) -> str:
    e = e.lower().strip()
    if e == "joy":
        return "positive"
    if e in {"fear", "anger", "guilt", "sadness"}:
        return "negative"
    return "neutral"


def load_culemo() -> List[CuRow]:
    """
    Tente de charger depuis HuggingFace; sinon attend un CSV local:
    data/culemo_all.csv avec colonnes:
    country,language,prompt_text,emotion_label,split
    """
    rows: List[CuRow] = []
    if HF_AVAILABLE:
        try:
            # Si le jeu est publié sous "llm-for-emotion/culemo" (cf. papier),
            # sinon adapte ce repo au besoin.
            ds = load_dataset("llm-for-emotion/culemo")
            # On concatène tout pour retrouver (country,language,split)
            full = ds["train"].flatten_indices()
            if "validation" in ds: full = concatenate_datasets([full, ds["validation"]])
            if "test" in ds:       full = concatenate_datasets([full, ds["test"]])

            cols = full.column_names
            # essai de noms typiques
            cand_text = "prompt_text" if "prompt_text" in cols else ("text" if "text" in cols else None)
            cand_label = "emotion_label" if "emotion_label" in cols else ("label" if "label" in cols else None)
            cand_lang = "language" if "language" in cols else ("lang" if "lang" in cols else None)
            cand_split = "split" if "split" in cols else None
            cand_ctry = "country" if "country" in cols else ("country_code" if "country_code" in cols else None)

            if not all([cand_text, cand_label, cand_lang, cand_split, cand_ctry]):
                raise ValueError("Colonnes inattendues dans le dataset HF, bascule CSV.")

            for ex in full:
                rows.append(CuRow(
                    country=str(ex[cand_ctry]).strip(),
                    language=str(ex[cand_lang]).strip().upper(),
                    text=str(ex[cand_text]),
                    emotion=str(ex[cand_label]).strip().lower(),
                    split=str(ex[cand_split]).strip()
                ))
            return rows
        except Exception:
            pass

    # Fallback CSV local
    path = "data/culemo_all.csv"
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Impossible de charger CuLEMo. Installe le dataset HF, ou fournis {path} "
            "avec colonnes: country,language,prompt_text,emotion_label,split"
        )
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for ex in r:
            rows.append(CuRow(
                country=ex["country"].strip(),
                language=ex["language"].strip().upper(),
                text=ex["prompt_text"],
                emotion=ex["emotion_label"].strip().lower(),
                split=ex["split"].strip()
            ))
    return rows


def subset(rows: List[CuRow], country: str, language: str, split: str) -> List[CuRow]:
    return [r for r in rows if r.country == country and r.language == language and r.split == split]


# =========================
# Baseline TF-IDF + LogReg
# =========================
@dataclass
class TfidfLogRegModel:
    vec: TfidfVectorizer
    clf: LogisticRegression
    labels: List[str]


def train_tfidf(X: List[str], y: List[str]) -> TfidfLogRegModel:
    vec = TfidfVectorizer(min_df=3, ngram_range=(1, 2), lowercase=True)
    Xv = vec.fit_transform(X)
    clf = LogisticRegression(max_iter=2000, class_weight="balanced")
    clf.fit(Xv, y)
    return TfidfLogRegModel(vec=vec, clf=clf, labels=sorted(set(y)))


def eval_acc(m: TfidfLogRegModel, X: List[str], y: List[str]) -> float:
    Xv = m.vec.transform(X)
    p = m.clf.predict(Xv)
    return float(accuracy_score(y, p))


# =========================
# LLM client
# =========================
class LLMClient:
    """
    Adapte ceci à ton endpoint (OpenAI, vLLM, TGI, etc.).
    Méthodes doivent retourner une étiquette dans l'espace attendu.
    """

    def predict_emotion(self, text: str, country_ctx: str, lang: str) -> str:
        raise NotImplementedError

    def predict_sentiment(self, text: str, country_ctx: str, lang: str) -> str:
        raise NotImplementedError


class DummyLLM(LLMClient):
    """
    Mock pour tester le pipeline sans API: prédit la classe majoritaire du train,
    et "bénéficie" légèrement du contexte pays.
    """

    def __init__(self, priors_per_lang: Dict[Tuple[str, str], Dict[str, int]]):
        self.priors = priors_per_lang  # {(country,lang): {label:count}}

    def _mode(self, country: str, lang: str, labels: List[str], boost_ctx: bool) -> str:
        pri = self.priors.get((country, lang), {})
        if not pri:
            return random.choice(labels)
        # petit "boost" contextuel: favorise la classe majoritaire si ctx=True
        items = list(pri.items())
        items.sort(key=lambda kv: kv[1], reverse=True)
        if boost_ctx and len(items) > 1:
            return items[0][0]
        return items[0][0]

    def predict_emotion(self, text: str, country_ctx: str, lang: str) -> str:
        # On ne connaît pas "country" ici -> on s'appuie sur un champ global si encodé dans country_ctx
        country = country_ctx.replace("You live in ", "").replace(",", "").strip() if country_ctx else "USA"
        labels = ["joy", "fear", "sadness", "anger", "guilt", "neutral"]
        return self._mode(country, lang, labels, boost_ctx=bool(country_ctx))

    def predict_sentiment(self, text: str, country_ctx: str, lang: str) -> str:
        country = country_ctx.replace("You live in ", "").replace(",", "").strip() if country_ctx else "USA"
        labels = ["positive", "negative", "neutral"]
        return self._mode(country, lang, labels, boost_ctx=bool(country_ctx))


# =========================
# Expérimentation
# =========================
def accuracy_llm(
        client: LLMClient, task: str, test_rows: List[CuRow], country_ctx: str, lang: str
) -> float:
    preds = []
    golds = []
    for r in test_rows:
        gold = r.emotion if task == "emotion" else to_sentiment(r.emotion)
        if task == "emotion":
            pred = client.predict_emotion(r.text, country_ctx, lang)
        else:
            pred = client.predict_sentiment(r.text, country_ctx, lang)
        preds.append(pred)
        golds.append(gold)
    return float(accuracy_score(golds, preds)) if preds else 0.0


def main():
    os.makedirs("out", exist_ok=True)
    rows = load_culemo()

    # Construire des "priors" (pour le DummyLLM)
    priors: Dict[Tuple[str, str], Dict[str, int]] = {}
    for c in ALL_COUNTRIES:
        nat = COUNTRY_NATIVE_LANG[c]
        tr = subset(rows, c, nat, "train")
        if tr:
            emo_counts = {}
            sent_counts = {}
            for r in tr:
                emo_counts[r.emotion] = emo_counts.get(r.emotion, 0) + 1
                s = to_sentiment(r.emotion)
                sent_counts[s] = sent_counts.get(s, 0) + 1
            priors[(c, nat)] = {**emo_counts, **sent_counts}

    # Remplace DummyLLM par ton client réel si besoin
    llm = DummyLLM(priors)

    out_path = "out/results.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["task", "country", "language", "approach", "country_ctx", "accuracy"])

        # Boucle sur tous les pays (au moins deux exigés par l’énoncé; ici on les fait tous)
        for country in ALL_COUNTRIES:
            nat = COUNTRY_NATIVE_LANG[country]

            # ====== Baselines en langue native ======
            for task in ["emotion", "sentiment"]:
                tr = subset(rows, country, nat, "train")
                te = subset(rows, country, nat, "test")
                if not tr or not te:
                    print(f"[WARN] No data for {country}-{nat} {task}")
                    continue

                Xtr = [r.text for r in tr]
                if task == "emotion":
                    ytr = [r.emotion for r in tr]
                    yte = [r.emotion for r in te]
                else:
                    ytr = [to_sentiment(r.emotion) for r in tr]
                    yte = [to_sentiment(r.emotion) for r in te]

                Xte = [r.text for r in te]
                m = train_tfidf(Xtr, ytr)
                acc = eval_acc(m, Xte, yte)
                w.writerow([task, country, nat, "TFIDF-LogReg", "no", f"{acc:.4f}"])
                print(f"[{task}][{country}-{nat}][TFIDF] acc={acc:.4f}")

            # ====== LLM zero/few-shot : 4 variantes ======
            # On évalue sur le même test natif (les textes ne changent pas; c’est le prompt côté LLM qui change)
            te_nat = subset(rows, country, nat, "test")
            if te_nat:
                for task in ["emotion", "sentiment"]:
                    # A) natif, sans contexte
                    acc = accuracy_llm(llm, task, te_nat, country_ctx="", lang=nat)
                    w.writerow([task, country, nat, "LLM-zero", "no", f"{acc:.4f}"])
                    # B) natif, avec contexte
                    ctx = f"You live in {country}, "
                    acc = accuracy_llm(llm, task, te_nat, country_ctx=ctx, lang=nat)
                    w.writerow([task, country, nat, "LLM-zero", "yes", f"{acc:.4f}"])
                    # C) anglais, sans contexte
                    acc = accuracy_llm(llm, task, te_nat, country_ctx="", lang="EN")
                    w.writerow([task, country, "EN", "LLM-zero", "no", f"{acc:.4f}"])
                    # D) anglais, avec contexte
                    acc = accuracy_llm(llm, task, te_nat, country_ctx=ctx, lang="EN")
                    w.writerow([task, country, "EN", "LLM-zero", "yes", f"{acc:.4f}"])

    print(f"\nRésultats écrits dans {out_path}")


if __name__ == "__main__":
    main()
