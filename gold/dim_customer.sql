
```

---

**Ce que ça change fondamentalement :**

Avant (incorrect) — jointure avec `is_current = TRUE` sur toutes les dimensions :
```
customer v1 (2020) JOIN person is_current=TRUE (2024) → ❌ mauvaise adresse pour 2020
customer v2 (2022) JOIN person is_current=TRUE (2024) → ❌ mauvaise adresse pour 2022
customer v3 (2024) JOIN person is_current=TRUE (2024) → ✅ correct seulement pour le présent
```

Après (correct) — jointure temporelle `BETWEEN` :
```
customer v1 (2020) JOIN person valide en 2020 → ✅ bonne adresse pour 2020
customer v2 (2022) JOIN person valide en 2022 → ✅ bonne adresse pour 2022
customer v3 (2024) JOIN person valide en 2024 → ✅ bonne adresse pour 2024