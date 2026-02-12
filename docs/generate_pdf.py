"""
SnowGoal - PDF Documentation Generator
Clean, professional style without emojis
"""

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.colors import HexColor, white, black
from reportlab.lib.units import cm, mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, KeepTogether
)
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.lib import colors
import os

# Colors
PRIMARY = HexColor('#29B5E8')
SECONDARY = HexColor('#171B26')
ACCENT = HexColor('#7E57C2')
TEXT = HexColor('#333333')
TEXT_LIGHT = HexColor('#666666')
BG_LIGHT = HexColor('#f5f5f5')

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), 'SnowGoal_Documentation.pdf')

def create_styles():
    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle(
        name='CoverTitle',
        fontSize=36,
        textColor=SECONDARY,
        alignment=TA_CENTER,
        spaceAfter=15,
        fontName='Helvetica-Bold'
    ))

    styles.add(ParagraphStyle(
        name='CoverSubtitle',
        fontSize=16,
        textColor=TEXT_LIGHT,
        alignment=TA_CENTER,
        spaceAfter=30
    ))

    styles.add(ParagraphStyle(
        name='SectionTitle',
        fontSize=16,
        textColor=SECONDARY,
        spaceBefore=25,
        spaceAfter=15,
        fontName='Helvetica-Bold'
    ))

    styles.add(ParagraphStyle(
        name='SubSectionTitle',
        fontSize=12,
        textColor=ACCENT,
        spaceBefore=15,
        spaceAfter=10,
        fontName='Helvetica-Bold'
    ))

    styles.add(ParagraphStyle(
        name='Body',
        fontSize=10,
        textColor=TEXT,
        alignment=TA_JUSTIFY,
        spaceAfter=10,
        leading=14
    ))

    styles.add(ParagraphStyle(
        name='CodeText',
        fontSize=8,
        fontName='Courier',
        textColor=TEXT,
        leading=10,
        leftIndent=5,
        rightIndent=5
    ))

    return styles


def create_cover(styles):
    elements = []
    elements.append(Spacer(1, 6*cm))
    elements.append(Paragraph("SnowGoal", styles['CoverTitle']))
    elements.append(Paragraph("Football Analytics Pipeline", styles['CoverSubtitle']))
    elements.append(Paragraph("100% Snowflake Native", styles['CoverSubtitle']))
    elements.append(Spacer(1, 2*cm))

    # Tech stack
    tech_data = [['Snowflake', 'Snowpark Python', 'Dynamic Tables', 'Streamlit']]
    tech_table = Table(tech_data, colWidths=[4*cm]*4)
    tech_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), PRIMARY),
        ('TEXTCOLOR', (0,0), (-1,-1), white),
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('FONTNAME', (0,0), (-1,-1), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('TOPPADDING', (0,0), (-1,-1), 10),
        ('BOTTOMPADDING', (0,0), (-1,-1), 10),
    ]))
    elements.append(tech_table)

    elements.append(Spacer(1, 3*cm))

    # Leagues
    leagues_data = [
        ['Premier League (PL)', 'La Liga (PD)', 'Bundesliga (BL1)'],
        ['Serie A (SA)', 'Ligue 1 (FL1)', '']
    ]
    leagues_table = Table(leagues_data, colWidths=[5.3*cm]*3)
    leagues_table.setStyle(TableStyle([
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('FONTSIZE', (0,0), (-1,-1), 10),
        ('TOPPADDING', (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
        ('TEXTCOLOR', (0,0), (-1,-1), TEXT),
    ]))
    elements.append(leagues_table)

    elements.append(Spacer(1, 3*cm))

    info_data = [
        ['Source', 'football-data.org API'],
        ['Refresh', 'Automatique toutes les 6 heures'],
        ['Architecture', 'Medallion (RAW - SILVER - GOLD)']
    ]
    info_table = Table(info_data, colWidths=[4*cm, 10*cm])
    info_table.setStyle(TableStyle([
        ('FONTNAME', (0,0), (0,-1), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 10),
        ('TEXTCOLOR', (0,0), (-1,-1), TEXT),
        ('TOPPADDING', (0,0), (-1,-1), 5),
        ('BOTTOMPADDING', (0,0), (-1,-1), 5),
    ]))
    elements.append(info_table)

    elements.append(PageBreak())
    return elements


def create_toc(styles):
    elements = []
    elements.append(Paragraph("Table des Matieres", styles['SectionTitle']))

    toc_data = [
        ['1.', "Vue d'ensemble"],
        ['2.', 'Architecture'],
        ['3.', 'Features Snowflake'],
        ['4.', 'Modele de Donnees'],
        ['5.', 'Pipeline ETL'],
        ['6.', 'Ordre de Deploiement'],
        ['7.', 'Dashboard Streamlit'],
    ]
    toc_table = Table(toc_data, colWidths=[1*cm, 14*cm])
    toc_table.setStyle(TableStyle([
        ('FONTSIZE', (0,0), (-1,-1), 11),
        ('TEXTCOLOR', (0,0), (-1,-1), TEXT),
        ('TOPPADDING', (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
        ('LINEBELOW', (0,0), (-1,-1), 0.5, BG_LIGHT),
    ]))
    elements.append(toc_table)

    elements.append(PageBreak())
    return elements


def create_overview(styles):
    elements = []
    elements.append(Paragraph("1. Vue d'ensemble", styles['SectionTitle']))

    elements.append(Paragraph(
        "SnowGoal est un pipeline de donnees analytics pour le football europeen, "
        "construit entierement avec les fonctionnalites natives de Snowflake. "
        "Le projet demontre l'utilisation de 15+ features Snowflake dans un cas d'usage reel.",
        styles['Body']
    ))

    elements.append(Paragraph("Ligues Couvertes", styles['SubSectionTitle']))

    leagues_data = [
        ['Ligue', 'Code', 'Pays'],
        ['Premier League', 'PL', 'Angleterre'],
        ['La Liga', 'PD', 'Espagne'],
        ['Bundesliga', 'BL1', 'Allemagne'],
        ['Serie A', 'SA', 'Italie'],
        ['Ligue 1', 'FL1', 'France'],
    ]
    leagues_table = Table(leagues_data, colWidths=[6*cm, 3*cm, 5*cm])
    leagues_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), SECONDARY),
        ('TEXTCOLOR', (0,0), (-1,0), white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 10),
        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
        ('TOPPADDING', (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
        ('GRID', (0,0), (-1,-1), 0.5, colors.lightgrey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [white, BG_LIGHT]),
    ]))
    elements.append(leagues_table)

    return elements


def create_architecture(styles):
    elements = []
    elements.append(Paragraph("2. Architecture", styles['SectionTitle']))

    elements.append(Paragraph("Pipeline Flow", styles['SubSectionTitle']))

    flow_data = [['API', 'RAW', 'STAGING', 'SILVER', 'GOLD', 'Streamlit']]
    flow_table = Table(flow_data, colWidths=[2.5*cm]*6)
    flow_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), PRIMARY),
        ('TEXTCOLOR', (0,0), (-1,-1), white),
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('FONTNAME', (0,0), (-1,-1), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('TOPPADDING', (0,0), (-1,-1), 12),
        ('BOTTOMPADDING', (0,0), (-1,-1), 12),
    ]))
    elements.append(flow_table)

    elements.append(Spacer(1, 0.5*cm))

    elements.append(Paragraph("Composants", styles['SubSectionTitle']))

    arch_data = [
        ['Composant', 'Role'],
        ['API football-data.org', 'Source de donnees externe'],
        ['Snowpark Procedure', "Appel API avec gestion d'authentification"],
        ['External Access', 'Integration securisee avec API externe'],
        ['RAW (VARIANT)', 'Stockage JSON brut'],
        ['Streams', 'Capture des changements (CDC)'],
        ['STAGING (Views)', 'Transformation LATERAL FLATTEN'],
        ['SILVER (Tables)', 'Donnees nettoyees et typees'],
        ['GOLD (Dynamic Tables)', 'Agregations auto-refresh'],
        ['Tasks DAG', 'Orchestration automatique'],
        ['Streamlit', 'Dashboard interactif'],
    ]
    arch_table = Table(arch_data, colWidths=[5*cm, 11*cm])
    arch_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), SECONDARY),
        ('TEXTCOLOR', (0,0), (-1,0), white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
        ('GRID', (0,0), (-1,-1), 0.5, colors.lightgrey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [white, BG_LIGHT]),
    ]))
    elements.append(arch_table)

    elements.append(PageBreak())
    return elements


def create_features(styles):
    elements = []
    elements.append(Paragraph("3. Features Snowflake Utilisees", styles['SectionTitle']))

    features_data = [
        ['Feature', 'Description'],
        ['VARIANT Columns', 'Stockage JSON semi-structure sans schema fixe'],
        ['Streams (CDC)', 'Capture des changements en temps reel'],
        ['Dynamic Tables', 'Rafraichissement automatique des agregations'],
        ['Snowpark Python', 'Procedures stockees en Python'],
        ['External Access', 'Integration securisee avec APIs externes'],
        ['Secrets Management', 'Stockage securise des cles API'],
        ['Tasks & DAG', 'Orchestration native avec dependances'],
        ['MERGE', 'Chargement incremental avec upsert'],
        ['Streamlit in Snowflake', 'Dashboard natif sans infra externe'],
        ['RBAC', "Controle d'acces par roles"],
        ['Masking Policies', 'Protection des donnees sensibles'],
        ['Internal Stages', 'Stockage des fichiers Python et Streamlit'],
    ]
    features_table = Table(features_data, colWidths=[4.5*cm, 11.5*cm])
    features_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), SECONDARY),
        ('TEXTCOLOR', (0,0), (-1,0), white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
        ('TOPPADDING', (0,0), (-1,-1), 7),
        ('BOTTOMPADDING', (0,0), (-1,-1), 7),
        ('GRID', (0,0), (-1,-1), 0.5, colors.lightgrey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [white, BG_LIGHT]),
    ]))
    elements.append(features_table)

    elements.append(PageBreak())
    return elements


def create_data_model(styles):
    elements = []
    elements.append(Paragraph("4. Modele de Donnees", styles['SectionTitle']))

    elements.append(Paragraph("Schemas", styles['SubSectionTitle']))

    schemas_data = [
        ['Schema', 'Description', 'Objets'],
        ['RAW', 'Donnees brutes JSON', 'RAW_MATCHES, RAW_TEAMS, RAW_STANDINGS...'],
        ['STAGING', 'Views LATERAL FLATTEN', 'V_MATCHES, V_TEAMS, V_STANDINGS...'],
        ['SILVER', 'Tables nettoyees', 'MATCHES, TEAMS, STANDINGS, SCORERS...'],
        ['GOLD', 'Dynamic Tables', 'DT_LEAGUE_STANDINGS, DT_TOP_SCORERS...'],
        ['COMMON', 'Objets partages', 'Stages, Secrets, Procedures'],
    ]
    schemas_table = Table(schemas_data, colWidths=[2.5*cm, 4.5*cm, 9*cm])
    schemas_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), SECONDARY),
        ('TEXTCOLOR', (0,0), (-1,0), white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('TOPPADDING', (0,0), (-1,-1), 7),
        ('BOTTOMPADDING', (0,0), (-1,-1), 7),
        ('GRID', (0,0), (-1,-1), 0.5, colors.lightgrey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [white, BG_LIGHT]),
    ]))
    elements.append(schemas_table)

    elements.append(Spacer(1, 0.5*cm))

    elements.append(Paragraph("Tables Silver", styles['SubSectionTitle']))

    tables_data = [
        ['Table', 'Colonnes Cles', 'Description'],
        ['MATCHES', 'MATCH_ID, HOME_TEAM, AWAY_TEAM, SCORE', 'Matchs avec scores'],
        ['STANDINGS', 'TEAM_ID, POSITION, POINTS, WON, LOST', 'Classements'],
        ['TEAMS', 'TEAM_ID, TEAM_NAME, VENUE, COACH', 'Infos equipes'],
        ['SCORERS', 'PLAYER_ID, GOALS, ASSISTS, TEAM', 'Top buteurs'],
        ['COMPETITIONS', 'COMPETITION_CODE, NAME, AREA', 'Competitions'],
    ]
    tables_table = Table(tables_data, colWidths=[3*cm, 6*cm, 7*cm])
    tables_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), SECONDARY),
        ('TEXTCOLOR', (0,0), (-1,0), white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('TOPPADDING', (0,0), (-1,-1), 7),
        ('BOTTOMPADDING', (0,0), (-1,-1), 7),
        ('GRID', (0,0), (-1,-1), 0.5, colors.lightgrey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [white, BG_LIGHT]),
    ]))
    elements.append(tables_table)

    return elements


def create_pipeline(styles):
    elements = []
    elements.append(Paragraph("5. Pipeline ETL", styles['SectionTitle']))

    elements.append(Paragraph("Flux de Donnees", styles['SubSectionTitle']))

    pipeline_data = [
        ['Etape', 'Source', 'Destination', 'Methode'],
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
        ('TOPPADDING', (0,0), (-1,-1), 7),
        ('BOTTOMPADDING', (0,0), (-1,-1), 7),
        ('GRID', (0,0), (-1,-1), 0.5, colors.lightgrey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [white, BG_LIGHT]),
    ]))
    elements.append(pipeline_table)

    elements.append(Spacer(1, 0.5*cm))

    elements.append(Paragraph("Orchestration (Tasks DAG)", styles['SubSectionTitle']))

    dag_data = [
        ['Task', 'Schedule', 'Description'],
        ['TASK_FETCH_ALL_LEAGUES', 'Every 6 hours', 'Fetch 5 leagues (60s delay each)'],
        ['TASK_MERGE_TO_SILVER', 'After FETCH', 'MERGE into Silver tables'],
        ['Dynamic Tables', 'Auto (30min-1h)', 'Refresh automatique'],
    ]
    dag_table = Table(dag_data, colWidths=[5.5*cm, 3.5*cm, 7*cm])
    dag_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), SECONDARY),
        ('TEXTCOLOR', (0,0), (-1,0), white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('TOPPADDING', (0,0), (-1,-1), 7),
        ('BOTTOMPADDING', (0,0), (-1,-1), 7),
        ('GRID', (0,0), (-1,-1), 0.5, colors.lightgrey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [white, BG_LIGHT]),
    ]))
    elements.append(dag_table)

    elements.append(PageBreak())
    return elements


def create_deploy(styles):
    elements = []
    elements.append(Paragraph("6. Ordre de Deploiement", styles['SectionTitle']))

    deploy_data = [
        ['#', 'Script', 'Description'],
        ['1', '00_init/01_database.sql', 'Database, Schemas, Warehouses'],
        ['2', '00_init/02_file_formats.sql', 'JSON format, Stages'],
        ['3', '01_raw/01_tables.sql', 'Tables RAW (VARIANT)'],
        ['4', '01_raw/02_streams.sql', 'Streams CDC'],
        ['5', 'Creer Secret + Upload Python', 'FOOTBALL_API_KEY + fichiers .py'],
        ['6', '01_raw/03_stored_procedure.sql', 'Network Rule, Integration, Procedure'],
        ['7', '01_raw/04_fetch_all_procedure.sql', 'Procedure FETCH_ALL_LEAGUES'],
        ['8', 'CALL FETCH_ALL_LEAGUES()', 'Chargement initial (5 min)'],
        ['9', '02_staging/01_views.sql', 'Views FLATTEN'],
        ['10', '03_silver/01_tables.sql', 'Tables Silver'],
        ['11', '03_silver/02_merge.sql', 'MERGE RAW vers Silver'],
        ['12', '04_gold/01_dynamic_tables.sql', 'Dynamic Tables'],
        ['13', '05_tasks/01_tasks.sql', 'Tasks DAG'],
        ['14', '06_security/01_rbac.sql', 'RBAC + Masking'],
        ['15', '07_streamlit/01_deploy_app.sql', 'Dashboard Streamlit'],
    ]
    deploy_table = Table(deploy_data, colWidths=[1*cm, 6.5*cm, 8.5*cm])
    deploy_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), SECONDARY),
        ('TEXTCOLOR', (0,0), (-1,0), white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
        ('GRID', (0,0), (-1,-1), 0.5, colors.lightgrey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [white, BG_LIGHT]),
        ('ALIGN', (0,0), (0,-1), 'CENTER'),
    ]))
    elements.append(deploy_table)

    return elements


def create_dashboard(styles):
    elements = []
    elements.append(Paragraph("7. Dashboard Streamlit", styles['SectionTitle']))

    elements.append(Paragraph("Pages", styles['SubSectionTitle']))

    pages_data = [
        ['Page', 'Description'],
        ['Home', 'Vue d\'ensemble avec stats live'],
        ['Standings', 'Classements par ligue avec KPIs'],
        ['Top Scorers', 'Podium des meilleurs buteurs'],
        ['Matches', 'Resultats recents et prochains matchs'],
        ['Teams', 'Details equipes avec stats Home/Away'],
    ]
    pages_table = Table(pages_data, colWidths=[4*cm, 12*cm])
    pages_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), SECONDARY),
        ('TEXTCOLOR', (0,0), (-1,0), white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 10),
        ('TOPPADDING', (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
        ('GRID', (0,0), (-1,-1), 0.5, colors.lightgrey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [white, BG_LIGHT]),
    ]))
    elements.append(pages_table)

    elements.append(Spacer(1, 0.5*cm))

    elements.append(Paragraph("Acces", styles['SubSectionTitle']))
    elements.append(Paragraph(
        "Le dashboard est accessible via Streamlit in Snowflake : "
        "Snowsight > Streamlit > SNOWGOAL_DASHBOARD",
        styles['Body']
    ))

    return elements


def generate_pdf():
    doc = SimpleDocTemplate(
        OUTPUT_PATH,
        pagesize=A4,
        rightMargin=2*cm,
        leftMargin=2*cm,
        topMargin=2*cm,
        bottomMargin=2*cm
    )

    styles = create_styles()
    elements = []

    elements.extend(create_cover(styles))
    elements.extend(create_toc(styles))
    elements.extend(create_overview(styles))
    elements.extend(create_architecture(styles))
    elements.extend(create_features(styles))
    elements.extend(create_data_model(styles))
    elements.extend(create_pipeline(styles))
    elements.extend(create_deploy(styles))
    elements.extend(create_dashboard(styles))

    doc.build(elements)
    print(f"PDF generated: {OUTPUT_PATH}")


if __name__ == "__main__":
    generate_pdf()
