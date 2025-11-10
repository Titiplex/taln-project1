import random

import numpy as np
from datasets import load_dataset, Dataset, concatenate_datasets
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score

random.seed(42)
np.random.seed(42)

# 1) charger Lou (nécessite Internet)
ds = load_dataset("tresiwalde/lou")

# concat propre des splits
full = concatenate_datasets([ds["train"], ds["validation"], ds["test"]]).flatten_indices()


# utilitaire: split + filtrage
def subset_task(task_: str):
    return full.filter(lambda ex: ex["task"] == task_)


def split_view(d: Dataset, split: str, strategy: str | None):
    if strategy is None:
        return d.filter(lambda ex: ex["split"] == split and ex["variant"] == "original")
    else:
        return d.filter(
            lambda ex: ex["split"] == split and ex["variant"] == "reformulated" and ex["strategy"] == strategy)


# essaye de trouver une clé d’alignement
def find_pair_key(d: Dataset):
    for cand in ["pair_id", "id", "uid", "orig_id", "example_id"]:
        if cand in d.column_names:
            return cand
    return None  # on tombera sinon sur un alignement par index avec vérif de taille


def align_pairs(testO: Dataset, testR: Dataset, key: str | None):
    if key and key in testO.column_names and key in testR.column_names:
        mapO = {k: i for i, k in enumerate(testO[key])}
        idxO, idxR = [], []
        for j, k in enumerate(testR[key]):
            if k in mapO:
                idxO.append(mapO[k])
                idxR.append(j)
        return testO.select(idxO), testR.select(idxR)
    else:
        n = min(len(testO), len(testR))
        return testO.select(range(n)), testR.select(range(n))


tasks = ["germ_eval_toxicity", "x_stance"]
strategies = ["GenderStern", "Doppelnennung", "Neutral", "GenderDoppelpunkt", "GenderGap", "De-e"]

for task in tasks:
    dtask = subset_task(task)
    tr = split_view(dtask, "train", None)
    teO = split_view(dtask, "test", None)

    Xtr, ytr = tr["text"], tr["label"]
    XteO, yteO = teO["text"], teO["label"]

    vec = TfidfVectorizer(min_df=3, ngram_range=(1, 2), lowercase=True)
    Xtrv = vec.fit_transform(Xtr)
    XteOv = vec.transform(XteO)

    clf = LogisticRegression(max_iter=2000, class_weight="balanced", n_jobs=None, solver="lbfgs")
    clf.fit(Xtrv, ytr)

    pO = clf.predict(XteOv)
    f1O = f1_score(yteO, pO, average="macro")
    print(f"[TFIDF-LogReg][{task}] macroF1(original_test)={f1O:.4f}")
    # print(report) si tu veux du détail :
    # print(classification_report(yteO, pO, digits=3))

    # évalue et calcule flips pour chaque stratégie
    pair_key = find_pair_key(dtask)
    for s in strategies:
        teR = split_view(dtask, "test", strategy=s)
        if len(teR) == 0:
            print(f"[{task}][{s}] no test data")
            continue

        teO_aligned, teR_aligned = align_pairs(teO, teR, pair_key)

        XteRv = vec.transform(teR_aligned["text"])
        yteR = teR_aligned["label"]

        # F1 sur la version réécrite
        pR = clf.predict(XteRv)
        f1R = f1_score(yteR, pR, average="macro")

        # flip rate entre prédictions sur original vs réécrit (alignées 1-à-1)
        pO_aligned = clf.predict(vec.transform(teO_aligned["text"]))
        flips = np.mean(pO_aligned != pR) * 100.0

        dF1 = f1R - f1O
        print(f"[TFIDF-LogReg][{task}][{s}] F1_rew={f1R:.4f}  ΔF1={dF1:+.4f}  flipRate={flips:.2f}%")
