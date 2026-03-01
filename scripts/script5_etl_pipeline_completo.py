# =============================================================
# SCRIPT 5: ETL PIPELINE COMPLETO v3
# Corrigido: usa coluna UNIDADE (nome completo da unidade)
# Driver: pg8000 (sem problema de encoding no Windows PT-BR)
# =============================================================
import sys
import pandas as pd
from datetime import datetime

print("=" * 60)
print("SCRIPT 5: ETL PIPELINE - DADOS REAIS SESI/SENAI")
print("=" * 60)

try:
    import pg8000
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pg8000"])
    import pg8000

# =============================================================
# CONFIGURACOES
# =============================================================
ARQUIVO_EXCEL = r"C:\projetos\engenharia_dados_incidentes\01_dados_originais\Incidentes.xlsx"
DB_HOST     = "localhost"
DB_PORT     = 5434
DB_NAME     = "incidentes_dw"
DB_USER     = "postgres"
DB_PASS     = "6982"
SLA_MINUTOS = 240

# =============================================================
# PASSO 1: EXTRACT
# =============================================================
print("\nPasso 1: EXTRACT - Carregando Excel...")
try:
    df = pd.read_excel(ARQUIVO_EXCEL)
    print("Registros: " + str(len(df)))
    print("Colunas: " + str(list(df.columns)))
except FileNotFoundError:
    print("ERRO: Arquivo nao encontrado: " + ARQUIVO_EXCEL)
    sys.exit(1)

# =============================================================
# PASSO 2: TRANSFORM
# =============================================================
print("\nPasso 2: TRANSFORM - Limpando dados...")

# Colunas fixas baseadas no seu Excel real
col_data    = 'DATA_OCORRENCIA'
col_down    = 'DOWNTIME'
col_tipo    = 'TIPOS_INDISPONIBILIDADE'
col_impacto = 'TIPO_IMPACTO'
col_status  = 'STATUS'

# UNIDADE: combina UNIDADE + UNIDADES_SELECIONADAS (cobre 100% dos registros)
# UNIDADE tem 1365 registros, UNIDADES_SELECIONADAS tem 4529 - juntas cobrem tudo!
col_unidade = 'unidade_final'
df['unidade_final'] = df['UNIDADE'].fillna(df['UNIDADES_SELECIONADAS'])
df['unidade_final'] = df['unidade_final'].fillna('SEM INFORMACAO').astype(str).str.strip()

com_unidade = (df['unidade_final'] != 'SEM INFORMACAO').sum()
print("Unidades preenchidas: " + str(com_unidade) + "/" + str(len(df)) + " (" + str(round(com_unidade/len(df)*100,1)) + "%)")
amostra = df['unidade_final'].value_counts().head(5)
print("Top 5 unidades:")
for nome, qtd in amostra.items():
    print("  " + str(nome)[:50] + " -> " + str(qtd))

# Converter datas
df[col_data] = pd.to_datetime(df[col_data], errors='coerce')
df = df.dropna(subset=[col_data])
print("Registros com data valida: " + str(len(df)))

# Converter downtime
def downtime_min(val):
    if pd.isna(val): return 0.0
    s = str(val).strip()
    try:
        p = s.split(':')
        if len(p) >= 2:
            return round(int(p[0])*60 + int(p[1]) + (float(p[2]) if len(p)>2 else 0)/60, 2)
        return round(float(s), 2)
    except:
        return 0.0

df['downtime_min'] = df[col_down].apply(downtime_min) if col_down in df.columns else 0.0
df['sla_violado']  = df['downtime_min'] > SLA_MINUTOS
df['critico']      = df['downtime_min'] > SLA_MINUTOS * 2

# Limpar strings (unidade_final ja foi criada e limpa acima)
if col_tipo in df.columns:
    df[col_tipo] = df[col_tipo].fillna('NAO INFORMADO').astype(str).str.strip()
if col_impacto in df.columns:
    df[col_impacto] = df[col_impacto].fillna('NAO INFORMADO').astype(str).str.strip()

print("SLA violados: " + str(df['sla_violado'].sum()))

# =============================================================
# PASSO 3: CONECTAR
# =============================================================
print("\nPasso 3: Conectando no PostgreSQL (porta " + str(DB_PORT) + ")...")
try:
    conn = pg8000.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS
    )
    cursor = conn.cursor()
    print("Conectado!")
except Exception as e:
    print("ERRO: " + str(e))
    sys.exit(1)

# =============================================================
# LIMPAR DADOS ANTIGOS
# =============================================================
print("\nLimpando dados antigos...")
cursor.execute("DELETE FROM fato_incidentes")
cursor.execute("DELETE FROM fato_disponibilidade_diaria")
cursor.execute("DELETE FROM dim_tipos_problema")
cursor.execute("DELETE FROM dim_unidades")
cursor.execute("DELETE FROM dim_tempo")
conn.commit()
print("Tabelas limpas!")

# =============================================================
# PASSO 4: DIM_TEMPO
# =============================================================
print("\nPasso 4: Populando dim_tempo...")
NOMES_DIA = ['Segunda','Terca','Quarta','Quinta','Sexta','Sabado','Domingo']
NOMES_MES = ['','Janeiro','Fevereiro','Marco','Abril','Maio','Junho',
             'Julho','Agosto','Setembro','Outubro','Novembro','Dezembro']

datas = df[col_data].dt.date.unique()
for d in datas:
    if d is None: continue
    dt = datetime.combine(d, datetime.min.time())
    cursor.execute("""
        INSERT INTO dim_tempo (data,ano,mes,dia,trimestre,semestre,
            dia_semana,nome_dia_semana,nome_mes,eh_fim_semana,eh_feriado,semana_ano)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (data) DO NOTHING
    """, (d, dt.year, dt.month, dt.day,
          (dt.month-1)//3+1, 1 if dt.month<=6 else 2,
          dt.weekday(), NOMES_DIA[dt.weekday()], NOMES_MES[dt.month],
          dt.weekday()>=5, False, dt.isocalendar()[1]))
conn.commit()
print("Datas: " + str(len(datas)))

# =============================================================
# PASSO 5: DIM_UNIDADES
# =============================================================
print("\nPasso 5: Populando dim_unidades...")
if col_unidade and col_unidade in df.columns:
    unidades = df[col_unidade].unique()
    for u in unidades:
        uid  = str(u).strip()[:100]
        nome = str(u).strip()[:200]
        cursor.execute("""
            INSERT INTO dim_unidades (unidade_id, nome_unidade, eh_atual)
            VALUES (%s, %s, True)
            ON CONFLICT (unidade_id) DO NOTHING
        """, (uid, nome))
    conn.commit()
    print("Unidades: " + str(len(unidades)))
    # Mostrar amostra
    cursor.execute("SELECT nome_unidade FROM dim_unidades LIMIT 5")
    rows = cursor.fetchall()
    print("Amostra salva: " + str([r[0] for r in rows]))

# =============================================================
# PASSO 6: DIM_TIPOS_PROBLEMA
# =============================================================
print("\nPasso 6: Populando dim_tipos_problema...")
if col_tipo in df.columns and col_impacto in df.columns:
    combos = df[[col_tipo, col_impacto]].drop_duplicates()
    for _, row in combos.iterrows():
        cat  = str(row[col_tipo])[:100]
        imp  = str(row[col_impacto])[:50]
        cursor.execute("""
            INSERT INTO dim_tipos_problema (categoria, subcategoria, tipo_impacto, severidade, sla_minutos)
            VALUES (%s, %s, %s, %s, %s)
        """, (cat, cat, imp, 'MEDIO', SLA_MINUTOS))
    conn.commit()
    print("Tipos: " + str(len(combos)))
elif col_tipo in df.columns:
    tipos = df[col_tipo].unique()
    for t in tipos:
        cat = str(t)[:100]
        cursor.execute("""
            INSERT INTO dim_tipos_problema (categoria, subcategoria, tipo_impacto, severidade, sla_minutos)
            VALUES (%s, %s, %s, %s, %s)
        """, (cat, cat, 'NAO INFORMADO', 'MEDIO', SLA_MINUTOS))
    conn.commit()
    print("Tipos: " + str(len(tipos)))

# =============================================================
# PASSO 7: FATO_INCIDENTES
# =============================================================
print("\nPasso 7: Populando fato_incidentes...")
print("Processando " + str(len(df)) + " registros...")

inseridos = 0
erros     = 0

for i, row in df.iterrows():
    try:
        d = row[col_data].date()

        cursor.execute("SELECT tempo_key FROM dim_tempo WHERE data = %s", (d,))
        r = cursor.fetchone()
        if not r: continue
        tempo_key = r[0]

        if col_unidade and col_unidade in df.columns:
            uid = str(row[col_unidade]).strip()[:100]
        else:
            uid = 'SEM INFORMACAO'
        cursor.execute("SELECT unidade_key FROM dim_unidades WHERE unidade_id = %s", (uid,))
        r = cursor.fetchone()
        if not r:
            cursor.execute("SELECT unidade_key FROM dim_unidades LIMIT 1")
            r = cursor.fetchone()
        if not r: continue
        unidade_key = r[0]

        if col_tipo in df.columns:
            cat = str(row[col_tipo]).strip()[:100]
            if col_impacto in df.columns:
                imp = str(row[col_impacto]).strip()[:50]
                cursor.execute("""
                    SELECT tipo_problema_key FROM dim_tipos_problema
                    WHERE categoria = %s AND tipo_impacto = %s LIMIT 1
                """, (cat, imp))
            else:
                cursor.execute("SELECT tipo_problema_key FROM dim_tipos_problema WHERE categoria = %s LIMIT 1", (cat,))
        else:
            cursor.execute("SELECT tipo_problema_key FROM dim_tipos_problema LIMIT 1")
        r = cursor.fetchone()
        if not r: continue
        tipo_key = r[0]

        downtime  = float(row['downtime_min'])
        sla_v     = bool(row['sla_violado'])
        critico   = bool(row['critico'])
        disp      = round(max(0.0, (1440.0 - downtime) / 1440.0 * 100), 2)
        status_v  = str(row[col_status]).strip()[:50] if col_status in df.columns else 'RESOLVIDO'

        cursor.execute("""
            INSERT INTO fato_incidentes (
                tempo_key, unidade_key, tipo_problema_key,
                downtime_minutos, disponibilidade_percentual,
                foi_sla_violado, teve_impacto_critico, foi_comunicado,
                status, data_hora_inicio
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (tempo_key, unidade_key, tipo_key,
              downtime, disp, sla_v, critico, False,
              status_v, row[col_data]))

        inseridos += 1
        if inseridos % 500 == 0:
            conn.commit()
            print("  " + str(inseridos) + " inseridos...")

    except Exception as ex:
        erros += 1
        if erros <= 3:
            print("  Aviso: " + str(ex)[:100])

conn.commit()
print("Inseridos: " + str(inseridos) + " | Erros: " + str(erros))

# =============================================================
# PASSO 8: VALIDAR
# =============================================================
print("\nPasso 8: Validando...")

cursor.execute("SELECT COUNT(*) FROM dim_tempo")
print("  dim_tempo:          " + str(cursor.fetchone()[0]))

cursor.execute("SELECT COUNT(*) FROM dim_unidades")
qtd_un = cursor.fetchone()[0]
print("  dim_unidades:       " + str(qtd_un))

cursor.execute("SELECT COUNT(*) FROM dim_tipos_problema")
print("  dim_tipos_problema: " + str(cursor.fetchone()[0]))

cursor.execute("SELECT COUNT(*) FROM fato_incidentes")
total = cursor.fetchone()[0]
print("  fato_incidentes:    " + str(total))

cursor.execute("SELECT nome_unidade, COUNT(*) as qtd FROM vw_incidentes_completo GROUP BY nome_unidade ORDER BY qtd DESC LIMIT 5")
print("\nTop 5 unidades:")
for row in cursor.fetchall():
    print("  " + str(row[0])[:40] + " -> " + str(row[1]))

cursor.close()
conn.close()

print("\n" + "=" * 60)
print("ETL CONCLUIDO! " + str(total) + " incidentes carregados!")
print("=" * 60)
print("\nNo Power BI: clique em Atualizar para ver os novos dados.")
