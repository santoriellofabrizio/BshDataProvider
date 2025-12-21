# first line: 561
    @cache_bsh_data
    def get_etf_fx(
            self,
            isin_list: List[str],
            day: dt.date,
            currency: Optional[str] = None,
            **kwargs,
    ) -> Dict[str, Dict[str, Dict[str, float]]]:
        """
        Ottiene la composizione FX per una lista di ISIN.

        Args:
            isin_list: Lista di ISIN
            day: Data di riferimento
            currency: Filtro opzionale per valuta specifica
            **kwargs: Parametri aggiuntivi (fx_fxfwrd mode)

        Returns:
            {isin: {"FX_COMPOSITION": {currency: weight, ...}}}
            Garantisce presenza di tutti gli ISIN, anche se senza dati.
        """
        if not isin_list:
            return {}

        day = day or dt.date.today()
        placeholders, params = self._in_clause("id", isin_list)
        params["ref_date"] = day.strftime("%d-%m-%Y")
        table_name = "PCF_FX_COMPOSITION_ONLINE" if (today().date() - day).days <= 28 else "PCF_FX_COMPOSITION"

        query = f"""
            SELECT BSH_ID, CURRENCY, WEIGHT, WEIGHT_FX_FORWARD, REF_DATE
            FROM {table_name}
            WHERE BSH_ID IN ({placeholders})
              AND REF_DATE = (
                  SELECT MAX(REF_DATE)
                  FROM {table_name}
                  WHERE BSH_ID IN ({placeholders})
                    AND REF_DATE <= TO_DATE(:ref_date, 'DD-MM-YYYY')
              )
        """
        if currency:
            query += " AND CURRENCY = :currency"
            params["currency"] = currency

        data, cols = self.conn.execute_query(query, params)

        # 🆕 Inizializza con tutti gli ISIN richiesti
        fx_dict = {
            isin: {"FX_COMPOSITION": {}, "FX_FORWARD": {}}
            for isin in isin_list
        }

        # 🆕 Popola solo gli ISIN con dati
        for bsh_id, curr, w, wf, _ in data:
            if bsh_id in fx_dict:
                fx_dict[bsh_id]["FX_COMPOSITION"][curr] = w or 0
                fx_dict[bsh_id]["FX_FORWARD"][curr] = wf or 0

        # Gestione modalità output
        mode = kwargs.get("fx_fxfwrd", "both").lower()
        if mode == "fx":
            return {k: {"FX_COMPOSITION": v["FX_COMPOSITION"]} for k, v in fx_dict.items()}
        elif mode == "fxfwrd":
            return {k: {"FX_COMPOSITION": v["FX_FORWARD"]} for k, v in fx_dict.items()}
        elif mode == "both":
            merged = {}
            for isin, comp in fx_dict.items():
                merged_fx = {
                    c: comp["FX_COMPOSITION"].get(c, 0) + comp["FX_FORWARD"].get(c, 0)
                    for c in set(comp["FX_COMPOSITION"]) | set(comp["FX_FORWARD"])
                }
                merged[isin] = {"FX_COMPOSITION": merged_fx}
            return merged
        else:
            raise ValueError("Invalid fx_fxfwrd parameter (use 'fx', 'fxfwrd', or 'both').")
