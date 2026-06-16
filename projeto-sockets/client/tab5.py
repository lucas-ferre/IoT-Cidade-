# --------------------------------------------------------------------
# ABA 5: Mapa Interativo (Matriz)
# --------------------------------------------------------------------
with tab5:
    st.subheader("🗺️ Mapa Interativo da Universidade (Matriz 100x100)")
    
    if "device_history" not in st.session_state or not st.session_state.device_history:
        st.info("Nenhum dispositivo encontrado. Vá para a aba 'Fontes de Dados' e atualize a topologia.")
    else:
        df_map = pd.DataFrame(st.session_state.device_history)
        
        # Filtros
        col_filters1, col_filters2 = st.columns(2)
        with col_filters1:
            map_mode = st.radio("Modo de Visualização", ["Gráfico de Dispersão (Scatter)", "Mapa de Calor (Heatmap)"], horizontal=True)
        with col_filters2:
            device_types = df_map["type"].unique()
            selected_types = st.multiselect("Filtrar por Tipo de Sensor", 
                options=device_types, 
                default=device_types,
                format_func=lambda x: {
                    messages_pb2.DEVICE_TYPE_CAMERA: "Câmera (Python)",
                    messages_pb2.DEVICE_TYPE_TRAFFIC_LIGHT: "Semáforo (Java)",
                    messages_pb2.DEVICE_TYPE_LAMP_POST: "Poste Inteligente (Lua)",
                    messages_pb2.DEVICE_TYPE_ENV_STATION: "Estação Ambiental (C)"
                }.get(x, f"Desconhecido ({x})")
            )
        
        if selected_types:
            df_filtered = df_map[df_map["type"].isin(selected_types)].copy()
            
            # Map type to string for hover
            df_filtered["type_str"] = df_filtered["type"].map(lambda x: {
                    messages_pb2.DEVICE_TYPE_CAMERA: "Câmera",
                    messages_pb2.DEVICE_TYPE_TRAFFIC_LIGHT: "Semáforo",
                    messages_pb2.DEVICE_TYPE_LAMP_POST: "Poste Inteligente",
                    messages_pb2.DEVICE_TYPE_ENV_STATION: "Estação Ambiental"
                }.get(x, "Desconhecido")
            )
            df_filtered["status_str"] = df_filtered["status"].map(lambda x: {
                    messages_pb2.STATUS_ON: "ON",
                    messages_pb2.STATUS_OFF: "OFF",
                    messages_pb2.STATUS_ERROR: "ERROR"
                }.get(x, "UNKNOWN")
            )

            if df_filtered.empty:
                st.warning("Nenhum dispositivo corresponde aos filtros.")
            else:
                if map_mode == "Gráfico de Dispersão (Scatter)":
                    fig = px.scatter(
                        df_filtered, 
                        x="coord_x", 
                        y="coord_y", 
                        color="type_str",
                        hover_name="device_id",
                        hover_data={
                            "type_str": True,
                            "status_str": True,
                            "coord_x": True,
                            "coord_y": True,
                            "ip_address": True,
                            "aggregator_id": True
                        },
                        title="Localização dos Sensores (Matriz 100x100)",
                        labels={"coord_x": "Coordenada X", "coord_y": "Coordenada Y", "type_str": "Tipo"},
                        range_x=[0, 100],
                        range_y=[0, 100],
                        height=600
                    )
                    fig.update_traces(marker=dict(size=12, line=dict(width=2, color='DarkSlateGrey')))
                else:
                    fig = px.density_heatmap(
                        df_filtered, 
                        x="coord_x", 
                        y="coord_y", 
                        title="Densidade de Sensores (Heatmap)",
                        labels={"coord_x": "Coordenada X", "coord_y": "Coordenada Y"},
                        range_x=[0, 100],
                        range_y=[0, 100],
                        nbinsx=20,
                        nbinsy=20,
                        height=600
                    )
                
                # Make interactive via st.plotly_chart
                event = st.plotly_chart(fig, use_container_width=True, on_select="rerun")
                
                if map_mode == "Gráfico de Dispersão (Scatter)" and event and event.get("selection") and event["selection"]["points"]:
                    selected_point = event["selection"]["points"][0]
                    if "customdata" in selected_point:
                        # Obter device_id correspondente
                        idx = selected_point["pointIndex"]
                        device_info = df_filtered.iloc[idx]
                        
                        st.markdown("### Detalhes do Sensor Selecionado")
                        st.json({
                            "ID do Dispositivo": device_info["device_id"],
                            "Tipo": device_info["type_str"],
                            "Status": device_info["status_str"],
                            "Coordenadas": f"X:{device_info['coord_x']}, Y:{device_info['coord_y']}",
                            "IP": device_info["ip_address"],
                            "Agregador": device_info["aggregator_id"],
                            "Última Vez Visto": datetime.datetime.fromtimestamp(device_info["last_seen_timestamp"]).strftime('%Y-%m-%d %H:%M:%S') if device_info["last_seen_timestamp"] else "Desconhecido"
                        })
