"""
SnowGoal - PDF Documentation Generator
"""

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.colors import HexColor, white, black
from reportlab.lib.units import cm, mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, Image, ListFlowable, ListItem
)
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.pdfgen import canvas
from reportlab.lib import colors
import os

# Colors
PRIMARY = HexColor('#29B5E8')
SECONDARY = HexColor('#171B26')
ACCENT = HexColor('#7E57C2')
SUCCESS = HexColor('#4CAF50')
TEXT = HexColor('#333333')
TEXT_LIGHT = HexColor('#666666')
BG_LIGHT = HexColor('#f8f9fa')

# Output path
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), 'SnowGoal_Documentation.pdf')

def create_styles():
    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle(
        name='MainTitle',
        parent=styles['Title'],
        fontSize=32,
        textColor=white,
        alignment=TA_CENTER,
        spaceAfter=10,
        fontName='Helvetica-Bold'
    ))

    styles.add(ParagraphStyle(
        name='Subtitle',
        parent=styles['Normal'],
        fontSize=14,
        textColor=white,
        alignment=TA_CENTER,
        spaceAfter=20
    ))

    styles.add(ParagraphStyle(
        name='SectionTitle',
        parent=styles['Heading1'],
        fontSize=18,
        textColor=SECONDARY,
        spaceBefore=20,
        spaceAfter=12,
        borderColor=PRIMARY,
        borderWidth=2,
        borderPadding=5,
        fontName='Helvetica-Bold'
    ))

    styles.add(ParagraphStyle(
        name='SubSection',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=ACCENT,
        spaceBefore=15,
        spaceAfter=8,
        fontName='Helvetica-Bold'
    ))

    styles.add(ParagraphStyle(
        name='BodyTextCustom',
        parent=styles['Normal'],
        fontSize=10,
        textColor=TEXT,
        alignment=TA_JUSTIFY,
        spaceAfter=8,
        leading=14
    ))

    styles.add(ParagraphStyle(
        name='CodeBlock',
        parent=styles['Normal'],
        fontSize=9,
        fontName='Courier',
        textColor=TEXT,
        backColor=BG_LIGHT,
        leftIndent=10,
        rightIndent=10,
        spaceBefore=5,
        spaceAfter=5
    ))

    styles.add(ParagraphStyle(
        name='TableHeader',
        parent=styles['Normal'],
        fontSize=10,
        textColor=white,
        fontName='Helvetica-Bold',
        alignment=TA_CENTER
    ))

    styles.add(ParagraphStyle(
        name='FeatureTitle',
        parent=styles['Normal'],
        fontSize=11,
        textColor=SECONDARY,
        fontName='Helvetica-Bold',
        spaceAfter=3
    ))

    return styles


def create_header_footer(canvas, doc):
    canvas.saveState()

    # Footer
    canvas.setFont('Helvetica', 8)
    canvas.setFillColor(TEXT_LIGHT)
    canvas.drawString(2*cm, 1.5*cm, "SnowGoal - Football Analytics Pipeline")
    canvas.drawRightString(A4[0] - 2*cm, 1.5*cm, f"Page {doc.page}")

    # Footer line
    canvas.setStrokeColor(PRIMARY)
    canvas.setLineWidth(1)
    canvas.line(2*cm, 1.8*cm, A4[0] - 2*cm, 1.8*cm)

    canvas.restoreState()


def create_cover_page(styles):
    elements = []

    # Background rectangle for title area
    elements.append(Spacer(1, 3*cm))

    # Title
    elements.append(Paragraph("⚽ SnowGoal", styles['MainTitle']))
    elements.append(Paragraph("Football Analytics Pipeline", styles['Subtitle']))
    elements.append(Paragraph("100% Snowflake Native", styles['Subtitle']))

    elements.append(Spacer(1, 2*cm))

    # Badges
    badges_data = [['Snowflake', 'Snowpark Python', 'Dynamic Tables', 'Streamlit', 'Medallion']]
    badges_table = Table(badges_data, colWidths=[3*cm]*5)
    badges_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), PRIMARY),
        ('TEXTCOLOR', (0,0), (-1,-1), white),
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('FONTNAME', (0,0), (-1,-1), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('ROUNDEDCORNERS', [5,5,5,5]),
        ('TOPPADDING', (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
    ]))
    elements.append(badges_table)

    elements.append(Spacer(1, 3*cm))

    # Leagues
    leagues_data = [
        ['🏴󠁧󠁢󠁥󠁮󠁧󠁿 Premier League', '🇪🇸 La Liga', '🇩🇪 Bundesliga', '🇮🇹 Serie A', '🇫🇷 Ligue 1']
    ]
    leagues_table = Table(leagues_data, colWidths=[3.2*cm]*5)
    leagues_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), BG_LIGHT),
        ('TEXTCOLOR', (0,0), (-1,-1), TEXT),
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('TOPPADDING', (0,0), (-1,-1), 12),
        ('BOTTOMPADDING', (0,0), (-1,-1), 12),
        ('BOX', (0,0), (-1,-1), 1, PRIMARY),
    ]))
    elements.append(leagues_table)

    elements.append(Spacer(1, 4*cm))

    # Info box
    info_text = """
    <b>Source de données:</b> football-data.org API<br/>
    <b>Refresh:</b> Automatique toutes les 6 heures<br/>
    <b>Architecture:</b> Medallion (RAW → SILVER → GOLD)
    """
    elements.append(Paragraph(info_text, styles['BodyTextCustom']))

    elements.append(PageBreak())

    return elements


def create_toc(styles):
    elements = []

    elements.append(Paragraph("📑 Table des Matières", styles['SectionTitle']))
    elements.append(Spacer(1, 0.5*cm))

    toc_items = [
        "1. Vue d'ensemble",
        "2. Architecture",
        "3. Features Snowflake",
        "4. Modèle de Données",
        "5. Pipeline ETL",
        "6. Ordre de Déploiement",
        "7. Dashboard Streamlit"
    ]

    for item in toc_items:
        elements.append(Paragraph(f"• {item}", styles['BodyTextCustom']))

    elements.append(Spacer(1, 1*cm))

    return elements


def create_overview(styles):
    elements = []

    elements.append(Paragraph("1. Vue d'ensemble", styles['SectionTitle']))

    overview_text = """
    <b>SnowGoal</b> est un pipeline de données analytics pour le football européen,
    construit entièrement avec les fonctionnalités natives de Snowflake. Le projet démontre
    l'utilisation de 15+ features Snowflake dans un cas d'usage réel.
    """
    elements.append(Paragraph(overview_text, styles['BodyTextCustom']))

    elements.append(Paragraph("Ligues Couvertes", styles['SubSection']))

    leagues_data = [
        ['Drapeau', 'Ligue', 'Code'],
        ['🏴󠁧󠁢󠁥󠁮󠁧󠁿', 'Premier League', 'PL'],
        ['🇪🇸', 'La Liga', 'PD'],
        ['🇩🇪', 'Bundesliga', 'BL1'],
        ['🇮🇹', 'Serie A', 'SA'],
        ['🇫🇷', 'Ligue 1', 'FL1'],
    ]

    leagues_table = Table(leagues_data, colWidths=[2.5*cm, 6*cm, 3*cm])
    leagues_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), SECONDARY),
        ('TEXTCOLOR', (0,0), (-1,0), white),
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 10),
        ('BOTTOMPADDING', (0,0), (-1,0), 10),
        ('TOPPADDING', (0,0), (-1,0), 10),
        ('BACKGROUND', (0,1), (-1,-1), BG_LIGHT),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [white, BG_LIGHT]),
    ]))
    elements.append(leagues_table)

    elements.append(Spacer(1, 1*cm))

    return elements


def create_architecture(styles):
    elements = []

    elements.append(Paragraph("2. Architecture", styles['SectionTitle']))

    elements.append(Paragraph("Pipeline Flow", styles['SubSection']))

    flow_data = [['🌐 API', '→', '📦 RAW', '→', '🔄 STAGING', '→', '🥈 SILVER', '→', '🥇 GOLD', '→', '📊 Streamlit']]
    flow_table = Table(flow_data, colWidths=[2.2*cm, 0.7*cm, 2.2*cm, 0.7*cm, 2.2*cm, 0.7*cm, 2.2*cm, 0.7*cm, 2.2*cm, 0.7*cm, 2.2*cm])
    flow_table.setStyle(TableStyle([
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('TEXTCOLOR', (1,0), (1,0), PRIMARY),
        ('TEXTCOLOR', (3,0), (3,0), PRIMARY),
        ('TEXTCOLOR', (5,0), (5,0), PRIMARY),
        ('TEXTCOLOR', (7,0), (7,0), PRIMARY),
        ('TEXTCOLOR', (9,0), (9,0), PRIMARY),
    ]))
    elements.append(flow_table)

    elements.append(Spacer(1, 0.5*cm))

    elements.append(Paragraph("Diagramme", styles['SubSection']))

    arch_text = """
    <font face="Courier" size="8">
    ┌──────────────────────────────────────────────────────────────────┐<br/>
    │                      SNOWGOAL PIPELINE                           │<br/>
    ├──────────────────────────────────────────────────────────────────┤<br/>
    │                                                                  │<br/>
    │   API ──▶ RAW (VARIANT) ──▶ STAGING (Views) ──▶ SILVER (MERGE)  │<br/>
    │                                                      │           │<br/>
    │   Snowpark      Streams                              ▼           │<br/>
    │   Procedure      (CDC)                    GOLD (Dynamic Tables)  │<br/>
    │   + Secrets                                          │           │<br/>
    │                                                      ▼           │<br/>
    │                                          STREAMLIT DASHBOARD     │<br/>
    │                                                                  │<br/>
    │   ┌────────────────────────────────────────────────────────┐    │<br/>
    │   │ TASKS: FETCH_ALL_LEAGUES ──▶ MERGE_TO_SILVER (6h)     │    │<br/>
    │   └────────────────────────────────────────────────────────┘    │<br/>
    └──────────────────────────────────────────────────────────────────┘
    </font>
    """
    elements.append(Paragraph(arch_text, styles['CodeBlock']))

    elements.append(PageBreak())

    return elements


def create_features(styles):
    elements = []

    elements.append(Paragraph("3. Features Snowflake Utilisées", styles['SectionTitle']))

    features = [
        ("📊 VARIANT Columns", "Stockage JSON semi-structuré pour les données API brutes sans schéma fixe."),
        ("🔄 Streams (CDC)", "Capture des changements en temps réel sur les tables RAW pour le traitement incrémental."),
        ("⚡ Dynamic Tables", "Rafraîchissement automatique des agrégations sans orchestration externe."),
        ("🐍 Snowpark Python", "Procédures stockées en Python pour l'appel API et transformation des données."),
        ("🌐 External Access", "Intégration sécurisée avec l'API externe football-data.org."),
        ("🔐 Secrets Management", "Stockage sécurisé de la clé API sans exposition dans le code."),
        ("📅 Tasks & DAG", "Orchestration native avec dépendances entre tâches."),
        ("🔀 MERGE", "Chargement incrémental avec upsert pour éviter les doublons."),
        ("📈 Streamlit in Snowflake", "Dashboard natif sans infrastructure externe."),
        ("👥 RBAC", "Contrôle d'accès par rôles (Admin, Analyst, Viewer)."),
        ("🎭 Masking Policies", "Protection des données sensibles (ex: date de naissance)."),
        ("📁 Internal Stages", "Stockage des fichiers Python et Streamlit."),
    ]

    features_data = [['Feature', 'Description']]
    for feature, desc in features:
        features_data.append([feature, desc])

    features_table = Table(features_data, colWidths=[5*cm, 11*cm])
    features_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), SECONDARY),
        ('TEXTCOLOR', (0,0), (-1,0), white),
        ('ALIGN', (0,0), (0,-1), 'LEFT'),
        ('ALIGN', (1,0), (1,-1), 'LEFT'),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
        ('TOPPADDING', (0,0), (-1,-1), 8),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [white, BG_LIGHT]),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ]))
    elements.append(features_table)

    elements.append(PageBreak())

    return elements


def create_data_model(styles):
    elements = []

    elements.append(Paragraph("4. Modèle de Données", styles['SectionTitle']))

    elements.append(Paragraph("Schemas", styles['SubSection']))

    schemas_data = [
        ['Schema', 'Description', 'Objets'],
        ['RAW', 'Données brutes JSON', 'RAW_MATCHES, RAW_TEAMS, RAW_STANDINGS...'],
        ['STAGING', 'Views LATERAL FLATTEN', 'V_MATCHES, V_TEAMS, V_STANDINGS...'],
        ['SILVER', 'Tables nettoyées', 'MATCHES, TEAMS, STANDINGS, SCORERS...'],
        ['GOLD', 'Dynamic Tables', 'DT_LEAGUE_STANDINGS, DT_TOP_SCORERS...'],
        ['COMMON', 'Objets partagés', 'Stages, Secrets, Procedures, Integrations'],
    ]

    schemas_table = Table(schemas_data, colWidths=[3*cm, 5*cm, 8*cm])
    schemas_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), SECONDARY),
        ('TEXTCOLOR', (0,0), (-1,0), white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
        ('TOPPADDING', (0,0), (-1,-1), 8),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [white, BG_LIGHT]),
    ]))
    elements.append(schemas_table)

    elements.append(Spacer(1, 0.5*cm))

    elements.append(Paragraph("Tables Silver", styles['SubSection']))

    tables_data = [
        ['Table', 'Colonnes Clés', 'Description'],
        ['MATCHES', 'MATCH_ID, HOME_TEAM, AWAY_TEAM, SCORE', 'Tous les matchs avec scores'],
        ['STANDINGS', 'TEAM_ID, POSITION, POINTS, WON, LOST', 'Classements par ligue'],
        ['TEAMS', 'TEAM_ID, TEAM_NAME, VENUE, COACH', 'Informations équipes'],
        ['SCORERS', 'PLAYER_ID, GOALS, ASSISTS, TEAM', 'Top buteurs'],
        ['COMPETITIONS', 'COMPETITION_CODE, NAME, AREA', 'Infos compétitions'],
    ]

    tables_table = Table(tables_data, colWidths=[3*cm, 6*cm, 7*cm])
    tables_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), SECONDARY),
        ('TEXTCOLOR', (0,0), (-1,0), white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
        ('TOPPADDING', (0,0), (-1,-1), 8),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [white, BG_LIGHT]),
    ]))
    elements.append(tables_table)

    elements.append(Spacer(1, 1*cm))

    return elements


def create_pipeline(styles):
    elements = []

    elements.append(Paragraph("5. Pipeline ETL", styles['SectionTitle']))

    elements.append(Paragraph("Flux de Données", styles['SubSection']))

    pipeline_data = [
        ['Étape', 'Source', 'Destination', 'Méthode'],
        ['1. Extract', 'football-data.org', 'RAW.RAW_*', 'Snowpark + External Access'],
        ['2. Flatten', 'RAW.RAW_*', 'STAGING.V_*', 'Views + LATERAL FLATTEN'],
        ['3. Transform', 'STAGING.V_*', 'SILVER.*', 'MERGE (incremental)'],
        ['4. Aggregate', 'SILVER.*', 'GOLD.DT_*', 'Dynamic Tables (auto)'],
    ]

    pipeline_table = Table(pipeline_data, colWidths=[2.5*cm, 4*cm, 4*cm, 5.5*cm])
    pipeline_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), SECONDARY),
        ('TEXTCOLOR', (0,0), (-1,0), white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
        ('TOPPADDING', (0,0), (-1,-1), 8),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [white, BG_LIGHT]),
    ]))
    elements.append(pipeline_table)

    elements.append(Spacer(1, 0.5*cm))

    elements.append(Paragraph("Orchestration (Tasks DAG)", styles['SubSection']))

    dag_text = """
    <font face="Courier" size="9">
    TASK_FETCH_ALL_LEAGUES (Schedule: every 6 hours)<br/>
        │<br/>
        │   Fetch PL → PD → BL1 → SA → FL1<br/>
        │   (60s delay between each for rate limiting)<br/>
        │<br/>
        └──▶ TASK_MERGE_TO_SILVER (After: FETCH)<br/>
                  │<br/>
                  └──▶ Dynamic Tables (Auto-refresh: 30min - 1h)
    </font>
    """
    elements.append(Paragraph(dag_text, styles['CodeBlock']))

    elements.append(PageBreak())

    return elements


def create_deploy_order(styles):
    elements = []

    elements.append(Paragraph("6. Ordre de Déploiement", styles['SectionTitle']))

    deploy_data = [
        ['#', 'Script', 'Description'],
        ['1', '00_init/01_database.sql', 'Database, Schemas, Warehouses'],
        ['2', '00_init/02_file_formats.sql', 'JSON format, Stages'],
        ['3', '01_raw/01_tables.sql', 'Tables RAW (VARIANT)'],
        ['4', '01_raw/02_streams.sql', 'Streams CDC'],
        ['5', 'Créer Secret + Upload Python', 'FOOTBALL_API_KEY + fichiers .py'],
        ['6', '01_raw/03_stored_procedure.sql', 'Network Rule, Integration, Procedure'],
        ['7', '01_raw/04_fetch_all_procedure.sql', 'Procedure FETCH_ALL_LEAGUES'],
        ['8', 'CALL FETCH_ALL_LEAGUES()', 'Chargement initial (~5 min)'],
        ['9', '02_staging/01_views.sql', 'Views FLATTEN'],
        ['10', '03_silver/01_tables.sql', 'Tables Silver'],
        ['11', '03_silver/02_merge.sql', 'MERGE RAW → Silver'],
        ['12', '04_gold/01_dynamic_tables.sql', 'Dynamic Tables'],
        ['13', '05_tasks/01_tasks.sql', 'Tasks DAG'],
        ['14', '06_security/01_rbac.sql', 'RBAC + Masking'],
        ['15', '07_streamlit/01_deploy_app.sql', 'Dashboard Streamlit'],
    ]

    deploy_table = Table(deploy_data, colWidths=[1*cm, 7*cm, 8*cm])
    deploy_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), SECONDARY),
        ('TEXTCOLOR', (0,0), (-1,0), white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [white, BG_LIGHT]),
        ('ALIGN', (0,0), (0,-1), 'CENTER'),
    ]))
    elements.append(deploy_table)

    elements.append(Spacer(1, 1*cm))

    return elements


def create_dashboard(styles):
    elements = []

    elements.append(Paragraph("7. Dashboard Streamlit", styles['SectionTitle']))

    elements.append(Paragraph("Pages", styles['SubSection']))

    pages_data = [
        ['Page', 'Description'],
        ['🏠 Home', "Vue d'ensemble avec stats live (Matches, Teams, Scorers, Leagues)"],
        ['🏆 Standings', 'Classements par ligue avec KPIs et graphiques'],
        ['🎯 Top Scorers', 'Podium des meilleurs buteurs avec stats détaillées'],
        ['📅 Matches', 'Résultats récents et prochains matchs'],
        ['🏟️ Teams', 'Détails équipes avec stats Home/Away'],
    ]

    pages_table = Table(pages_data, colWidths=[3.5*cm, 12.5*cm])
    pages_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), SECONDARY),
        ('TEXTCOLOR', (0,0), (-1,0), white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 10),
        ('BOTTOMPADDING', (0,0), (-1,-1), 10),
        ('TOPPADDING', (0,0), (-1,-1), 10),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [white, BG_LIGHT]),
    ]))
    elements.append(pages_table)

    elements.append(Spacer(1, 0.5*cm))

    elements.append(Paragraph("Accès", styles['SubSection']))
    access_text = """
    Le dashboard est accessible via <b>Streamlit in Snowflake</b> :<br/>
    Snowsight → Streamlit → SNOWGOAL_DASHBOARD
    """
    elements.append(Paragraph(access_text, styles['BodyTextCustom']))

    elements.append(Spacer(1, 2*cm))

    # Footer
    footer_text = """
    <b>SnowGoal</b> - Football Analytics Pipeline<br/>
    Built 100% on Snowflake Native Features<br/>
    Data Source: football-data.org | Architecture: Medallion (RAW → SILVER → GOLD)
    """
    elements.append(Paragraph(footer_text, ParagraphStyle(
        name='Footer',
        fontSize=10,
        textColor=TEXT_LIGHT,
        alignment=TA_CENTER,
        spaceBefore=20
    )))

    return elements


def generate_pdf():
    doc = SimpleDocTemplate(
        OUTPUT_PATH,
        pagesize=A4,
        rightMargin=2*cm,
        leftMargin=2*cm,
        topMargin=2*cm,
        bottomMargin=2.5*cm
    )

    styles = create_styles()

    elements = []

    # Build document
    elements.extend(create_cover_page(styles))
    elements.extend(create_toc(styles))
    elements.extend(create_overview(styles))
    elements.extend(create_architecture(styles))
    elements.extend(create_features(styles))
    elements.extend(create_data_model(styles))
    elements.extend(create_pipeline(styles))
    elements.extend(create_deploy_order(styles))
    elements.extend(create_dashboard(styles))

    doc.build(elements, onFirstPage=create_header_footer, onLaterPages=create_header_footer)

    print(f"PDF generated: {OUTPUT_PATH}")


if __name__ == "__main__":
    generate_pdf()
