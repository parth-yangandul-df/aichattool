#!/usr/bin/env python3
"""
Seed semantic metadata for your Azure SQL Server connection into QueryWise.

This script populates the four layers of the semantic layer:
  1. Glossary  — business terms → definitions + SQL expressions
  2. Metrics   — named KPIs with SQL expressions and dimensions
  3. Dictionary — raw column values → human-readable labels
  4. Knowledge  — free-text business context documents

Usage:
    # From the repo root, while docker compose is running:
    docker compose exec backend python scripts/seed_sqlserver_metadata.py

    # Or locally (needs httpx):
    pip install httpx
    python backend/scripts/seed_sqlserver_metadata.py

    # Target a different backend URL:
    python backend/scripts/seed_sqlserver_metadata.py --base-url http://localhost:8000

    # Target a specific connection by name (default: picks the first sqlserver connection):
    python backend/scripts/seed_sqlserver_metadata.py --connection-name "My Azure DB"

HOW TO FILL THIS IN:
    1. Run introspection in the UI (Connections → introspect icon) so your
       tables and columns are cached.
    2. Edit GLOSSARY_TERMS, METRICS, DICTIONARY_ENTRIES, and KNOWLEDGE_DOCS
       below to match your schema.
    3. Run this script — it is idempotent-safe (will report errors on duplicate
       unique constraint violations but won't crash).

DICTIONARY ENTRIES NOTE:
    Dictionary entries are keyed by (table_name, column_name). The table_name
    must match exactly what was introspected (case-sensitive). Run:
        GET /api/v1/connections/{id}/tables
    to see the exact table names that were cached.
"""

from __future__ import annotations

import argparse
import sys

import httpx

API_PREFIX = "/api/v1"


# =============================================================================
# 1. GLOSSARY TERMS
#    Business terms that map natural language to SQL concepts.
#
#    Fields:
#      term            (required) Short business term label
#      definition      (required) Human-readable explanation
#      sql_expression  (required) SQL snippet that implements this term
#      related_tables  (optional) List of table names this term touches
#      related_columns (optional) List of "table.column" strings
#      examples        (optional) List of example SQL queries
# =============================================================================
GLOSSARY_TERMS: list[dict] = [
    {
        "term": "Resource",
        "definition": "An individual employee or workforce member who performs tasks and logs work in the system.",
        "sql_expression": "Resource.ResourceId",
        "related_tables": ["Resource"],
        "related_columns": ["Resource.ResourceId"],
        "examples": [
            "SELECT * FROM Resource WHERE IsActive = 1"
        ],
    },
    {
        "term": "Active Resource",
        "definition": "A resource who is currently active and part of the organization.",
        "sql_expression": "CASE WHEN Resource.IsActive = 1 THEN 1 ELSE 0 END",
        "related_tables": ["Resource"],
        "related_columns": ["Resource.IsActive"],
    },
    {
        "term": "Employee ID",
        "definition": "Unique identifier assigned to each employee within the organization.",
        "sql_expression": "Resource.EmployeeId",
        "related_tables": ["Resource"],
        "related_columns": ["Resource.EmployeeId"],
    },
    {
        "term": "Resource Tenure (Months)",
        "definition": "Total duration of employment for a resource measured in months.",
        "sql_expression": "Resource.TenureInMonths",
        "related_tables": ["Resource"],
        "related_columns": ["Resource.TenureInMonths"],
    },
    {
        "term": "Resource Tenure (Years)",
        "definition": "Total duration of employment for a resource measured in years.",
        "sql_expression": "Resource.TenureInYears",
        "related_tables": ["Resource"],
        "related_columns": ["Resource.TenureInYears"],
    },
    {
        "term": "Reporting Manager",
        "definition": "The manager to whom the resource reports within the organization hierarchy.",
        "sql_expression": "Resource.ReportingTo",
        "related_tables": ["Resource"],
        "related_columns": ["Resource.ReportingTo"],
    },
    {
        "term": "Primary Skill",
        "definition": "Main technical or functional skill of the resource.",
        "sql_expression": "Resource.Primaryskill",
        "related_tables": ["Resource"],
        "related_columns": ["Resource.Primaryskill"],
    },
    {
        "term": "Secondary Skill",
        "definition": "Additional supporting skill set of the resource.",
        "sql_expression": "Resource.Secondaryskill",
        "related_tables": ["Resource"],
        "related_columns": ["Resource.Secondaryskill"],
    },
    {
        "term": "Date of Joining",
        "definition": "The date on which the resource joined the organization.",
        "sql_expression": "Resource.DateOfJoin",
        "related_tables": ["Resource"],
        "related_columns": ["Resource.DateOfJoin"],
    },
    {
        "term": "Client",
        "definition": "An external organization or customer for whom services are delivered.",
        "sql_expression": "Client.ClientId",
        "related_tables": ["Client"],
        "related_columns": ["Client.ClientId"],
        "examples": [
            "SELECT * FROM Client WHERE IsActive = 1"
        ],
    },
    {
        "term": "Active Client",
        "definition": "A client that is currently active and engaged in business operations.",
        "sql_expression": "CASE WHEN Client.IsActive = 1 THEN 1 ELSE 0 END",
        "related_tables": ["Client"],
        "related_columns": ["Client.IsActive"],
    },
    {
        "term": "Client Billing Rate (Hourly)",
        "definition": "Hourly rate charged to the client for services rendered.",
        "sql_expression": "Client.HourlyBillingRate",
        "related_tables": ["Client"],
        "related_columns": ["Client.HourlyBillingRate"],
    },
    {
        "term": "Agreement Value",
        "definition": "Total monetary value agreed upon in the client contract.",
        "sql_expression": "Client.AgreementValue",
        "related_tables": ["Client"],
        "related_columns": ["Client.AgreementValue"],
    },
    {
        "term": "Agreement Duration",
        "definition": "Duration of the contractual agreement with the client.",
        "sql_expression": "Client.AgreementDuration",
        "related_tables": ["Client"],
        "related_columns": ["Client.AgreementDuration"],
    },
    {
        "term": "Client Start Date",
        "definition": "The actual start date of engagement with the client.",
        "sql_expression": "Client.ActualStartDate",
        "related_tables": ["Client"],
        "related_columns": ["Client.ActualStartDate"],
    },
    {
        "term": "Client End Date",
        "definition": "The actual end date of engagement with the client.",
        "sql_expression": "Client.ActualEndDate",
        "related_tables": ["Client"],
        "related_columns": ["Client.ActualEndDate"],
    },
    {
        "term": "Business Unit",
        "definition": "An organizational division responsible for a specific business function or service line.",
        "sql_expression": "BusinessUnit.BusinessUnitId",
        "related_tables": ["BusinessUnit"],
        "related_columns": ["BusinessUnit.BusinessUnitId"],
    },
    {
        "term": "Active Business Unit",
        "definition": "A business unit that its currently operational and active.",
        "sql_expression": "CASE WHEN BusinessUnit.IsActive = 1 THEN 1 ELSE 0 END",
        "related_tables": ["BusinessUnit"],
        "related_columns": ["BusinessUnit.IsActive"],
    },
    {
        "term": "City",
        "definition": "Geographical city associated with clients or resources.",
        "sql_expression": "cities.cityID",
        "related_tables": ["cities"],
    },
    {
        "term": "Client Stakeholder",
        "definition": "An individual associated with a client organization who is involved in communication, decision-making, or project oversight.",
        "sql_expression": "ClientStakeholder.ClientStakeholderId",
        "related_tables": ["ClientStakeholder"],
        "related_columns": ["ClientStakeholder.ClientStakeholderId"],
    },
    {
        "term": "Active Client Stakeholder",
        "definition": "A stakeholder who is currently active and associated with a client.",
        "sql_expression": "CASE WHEN ClientStakeholder.IsActive = 1 THEN 1 ELSE 0 END",
        "related_tables": ["ClientStakeholder"],
        "related_columns": ["ClientStakeholder.IsActive"],
    },
    {
        "term": "Client Stakeholder Contact",
        "definition": "Contact information (email and phone) of a stakeholder representing a client.",
        "sql_expression": "ClientStakeholder.EmailId",
        "related_tables": ["ClientStakeholder"],
        "related_columns": ["ClientStakeholder.EmailId", "ClientStakeholder.ContactNumber"],
    },
    {
        "term": "Company Type",
        "definition": "Classification of a client organization based on its business nature or structure.",
        "sql_expression": "CompanyType.CompanyTypeId",
        "related_tables": ["CompanyType"],
        "related_columns": ["CompanyType.CompanyTypeId"],
    },
    {
        "term": "Active Company Type",
        "definition": "A company type that is currently valid and in use.",
        "sql_expression": "CASE WHEN CompanyType.IsActive = 1 THEN 1 ELSE 0 END",
        "related_tables": ["CompanyType"],
        "related_columns": ["CompanyType.IsActive"],
    },
    {
        "term": "Designated Role",
        "definition": "A formally assigned role that defines responsibilities and function of a resource within the organization.",
        "sql_expression": "DesignatedRole.DesignatedRoleId",
        "related_tables": ["DesignatedRole"],
        "related_columns": ["DesignatedRole.DesignatedRoleId"],
    },
    {
        "term": "Active Designated Role",
        "definition": "A designated role that is currently active and assigned within the organization.",
        "sql_expression": "CASE WHEN DesignatedRole.IsActive = 1 THEN 1 ELSE 0 END",
        "related_tables": ["DesignatedRole"],
        "related_columns": ["DesignatedRole.IsActive"],
    },
    {
        "term": "Designation",
        "definition": "Job title assigned to a resource indicating their position in the organizational hierarchy.",
        "sql_expression": "Designation.DesignationId",
        "related_tables": ["Designation"],
        "related_columns": ["Designation.DesignationId"],
    },
    {
        "term": "Timesheet Applicable Designation",
        "definition": "A designation for which timesheet (EOD logging) is mandatory.",
        "sql_expression": "CASE WHEN Designation.IsTimesheetApplies = 1 THEN 1 ELSE 0 END",
        "related_tables": ["Designation"],
        "related_columns": ["Designation.IsTimesheetApplies"],
    },
    {
        "term": "Active Designation",
        "definition": "A designation that is currently active in the system.",
        "sql_expression": "CASE WHEN Designation.IsActive = 1 THEN 1 ELSE 0 END",
        "related_tables": ["Designation"],
        "related_columns": ["Designation.IsActive"],
    },
    {
        "term": "Project",
        "definition": "A client engagement or initiative under which work is planned, executed, and tracked.",
        "sql_expression": "Project.ProjectId",
        "related_tables": ["Project"],
        "related_columns": ["Project.ProjectId"],
        "examples": [
            "SELECT * FROM Project WHERE IsActive = 1"
        ],
    },
    {
        "term": "Active Project",
        "definition": "A project that is currently active and ongoing.",
        "sql_expression": "CASE WHEN Project.IsActive = 1 THEN 1 ELSE 0 END",
        "related_tables": ["Project"],
        "related_columns": ["Project.IsActive"],
    },
    {
        "term": "Project Duration",
        "definition": "Planned duration of the project.",
        "sql_expression": "Project.Duration",
        "related_tables": ["Project"],
        "related_columns": ["Project.Duration"],
    },
    {
        "term": "Project Billing Rate (Hourly)",
        "definition": "Hourly billing rate defined for the project.",
        "sql_expression": "Project.HourlyBillingRate",
        "related_tables": ["Project"],
        "related_columns": ["Project.HourlyBillingRate"],
    },
    {
        "term": "Project Start Date",
        "definition": "Planned start date of the project.",
        "sql_expression": "Project.StartDate",
        "related_tables": ["Project"],
        "related_columns": ["Project.StartDate"],
    },
    {
        "term": "Project End Date",
        "definition": "Planned end date of the project.",
        "sql_expression": "Project.EndDate",
        "related_tables": ["Project"],
        "related_columns": ["Project.EndDate"],
    },
    {
        "term": "Actual Project Start Date",
        "definition": "Actual date when project execution began.",
        "sql_expression": "Project.ActualStartDate",
        "related_tables": ["Project"],
        "related_columns": ["Project.ActualStartDate"],
    },
    {
        "term": "Actual Project End Date",
        "definition": "Actual date when project execution completed.",
        "sql_expression": "Project.ActualEndDate",
        "related_tables": ["Project"],
        "related_columns": ["Project.ActualEndDate"],
    },
    {
        "term": "Project Resource Allocation",
        "definition": "Assignment of a resource to a project with defined allocation percentage and billing attributes.",
        "sql_expression": "ProjectResource.ProjectResourceId",
        "related_tables": ["ProjectResource"],
        "related_columns": ["ProjectResource.ProjectResourceId"],
    },
    {
        "term": "Billable Resource",
        "definition": "A resource whose work is billable to the client.",
        "sql_expression": "CASE WHEN ProjectResource.Billable = 1 THEN 1 ELSE 0 END",
        "related_tables": ["ProjectResource"],
        "related_columns": ["ProjectResource.Billable"],
    },
    {
        "term": "Shadow Resource",
        "definition": "A resource assigned for support or learning purposes and not directly billable.",
        "sql_expression": "CASE WHEN ProjectResource.Shadow = 1 THEN 1 ELSE 0 END",
        "related_tables": ["ProjectResource"],
        "related_columns": ["ProjectResource.Shadow"],
    },
    {
        "term": "Resource Allocation Percentage",
        "definition": "Percentage of a resource's time allocated to a project.",
        "sql_expression": "ProjectResource.PercentageAllocation",
        "related_tables": ["ProjectResource"],
        "related_columns": ["ProjectResource.PercentageAllocation"],
    },
    {
        "term": "Bench Resource",
        "definition": "A resource not currently allocated to billable work.",
        "sql_expression": "CASE WHEN ProjectResource.Bench = 1 THEN 1 ELSE 0 END",
        "related_tables": ["ProjectResource"],
        "related_columns": ["ProjectResource.Bench"],
    },
    {
        "term": "Client Status",
        "definition": "Operational status of a client such as Active, Inactive, or Closed.",
        "sql_expression": "Status.StatusName",
        "related_tables": ["Status"],
        "related_columns": ["Status.StatusName", "Status.ReferenceId"],
        "examples": [
            "SELECT * FROM Status WHERE ReferenceId = 1"
        ],
    },
    {
        "term": "Project Status",
        "definition": "Lifecycle status of a project such as Active, Inactive, On-hold, or Closed.",
        "sql_expression": "Status.StatusName",
        "related_tables": ["Status"],
        "related_columns": ["Status.StatusName", "Status.ReferenceId"],
        "examples": [
            "SELECT * FROM Status WHERE ReferenceId = 2"
        ],
    },
    {
        "term": "Resource Status",
        "definition": "Availability status of a resource such as Active or Inactive.",
        "sql_expression": "Status.StatusName",
        "related_tables": ["Status"],
        "related_columns": ["Status.StatusName", "Status.ReferenceId"],
    },
    {
        "term": "Email Queue Status",
        "definition": "Processing status of email queue items such as Pending, Success, or Error.",
        "sql_expression": "Status.StatusName",
        "related_tables": ["Status"],
        "related_columns": ["Status.StatusName", "Status.ReferenceId"],
    },
    {
        "term": "Active Status",
        "definition": "A status value indicating an entity is currently active.",
        "sql_expression": "CASE WHEN Status.StatusName = 'Active' THEN 1 ELSE 0 END",
        "related_tables": ["Status"],
        "related_columns": ["Status.StatusName"],
    },
    {
        "term": "Technology Category",
        "definition": "A classification grouping technologies or skills into broader categories such as Frontend, Backend, Data Engineering, etc.",
        "sql_expression": "TechCatagory.TechCategoryId",
        "related_tables": ["TechCatagory"],
        "related_columns": ["TechCatagory.TechCategoryId"],
    },
    {
        "term": "Technology Category Name",
        "definition": "The name representing a specific technology category.",
        "sql_expression": "TechCatagory.TechCategoryName",
        "related_tables": ["TechCatagory"],
        "related_columns": ["TechCatagory.TechCategoryName"],
    },
    {
        "term": "Active Technology Category",
        "definition": "A technology category that is currently active and available for use.",
        "sql_expression": "CASE WHEN TechCatagory.IsActive = 1 THEN 1 ELSE 0 END",
        "related_tables": ["TechCatagory"],
        "related_columns": ["TechCatagory.IsActive"],
    },
    {
        "term": "Technology Category Function Mapping",
        "definition": "Mapping of a technology category to one or more functions using FunctionIds.",
        "sql_expression": "TechCatagory.FunctionIds",
        "related_tables": ["TechCatagory"],
        "related_columns": ["TechCatagory.FunctionIds"],
    },
    {
        "term": "Technology Function",
        "definition": "A functional grouping of technology roles such as Development, QA, DevOps, or Data.",
        "sql_expression": "TechFunction.FunctionId",
        "related_tables": ["TechFunction"],
        "related_columns": ["TechFunction.FunctionId"],
    },
    {
        "term": "Technology Function Name",
        "definition": "The name of the functional technology group.",
        "sql_expression": "TechFunction.FunctionName",
        "related_tables": ["TechFunction"],
        "related_columns": ["TechFunction.FunctionName"],
    },
    {
        "term": "Active Technology Function",
        "definition": "A technology function that is currently active and usable.",
        "sql_expression": "CASE WHEN TechFunction.IsActive = 1 THEN 1 ELSE 0 END",
        "related_tables": ["TechFunction"],
        "related_columns": ["TechFunction.IsActive"],
    },
    {
        "term": "Function Organization Mapping",
        "definition": "Mapping of technology function to one or more organizations.",
        "sql_expression": "TechFunction.OrganizationIds",
        "related_tables": ["TechFunction"],
        "related_columns": ["TechFunction.OrganizationIds"],
    },
    {
    "term": "Client Name",
    "definition": "The name of the client, stored in Client.ClientName. This should always be used when referring to a client.",
    "sql_expression": "Client.ClientName",
    "related_tables": ["Client"],
    "related_columns": ["Client.ClientName"]
},
{
    "term": "Business Unit Name",
    "definition": "The name of the business unit, stored in BusinessUnit.BusinessUnitName.",
    "sql_expression": "BusinessUnit.BusinessUnitName",
    "related_tables": ["BusinessUnit"],
    "related_columns": ["BusinessUnit.BusinessUnitName"]
},
{
    "term": "Resource Name",
    "definition": "The name of the employee or resource, stored in Resource.ResourceName.",
    "sql_expression": "Resource.ResourceName",
    "related_tables": ["Resource"],
    "related_columns": ["Resource.ResourceName"]
},
{
    "term": "Employee",
    "definition": "An employee refers to a resource in the system.",
    "sql_expression": "Resource.ResourceId",
    "related_tables": ["Resource"]
}


    # -------------------------------------------------------------------------
    # TODO: Replace these examples with terms from your domain.
    #
    # Example for a sales database:
    # {
    #     "term": "ARR",
    #     "definition": "Annual Recurring Revenue — annualised value of active subscriptions.",
    #     "sql_expression": "SUM(subscriptions.monthly_value) * 12",
    #     "related_tables": ["subscriptions"],
    #     "related_columns": ["subscriptions.monthly_value"],
    #     "examples": [
    #         "SELECT SUM(monthly_value) * 12 AS arr FROM subscriptions WHERE status = 'active'",
    #     ],
    # },
    # -------------------------------------------------------------------------
]


# =============================================================================
# 2. METRICS
#    Named, reusable KPI definitions. The LLM uses these to answer
#    "what is our X?" questions correctly without needing to figure out
#    the SQL from scratch.
#
#    Fields:
#      metric_name      (required) snake_case identifier
#      display_name     (required) Human-readable name shown in context
#      description      (optional) Longer explanation
#      sql_expression   (required) SQL fragment that computes the metric
#      aggregation_type (optional) "sum" | "avg" | "count" | "ratio" | "max" | "min"
#      related_tables   (optional) List of table names
#      dimensions       (optional) Columns typically used to GROUP BY this metric
#      filters          (optional) Dict of pre-applied WHERE conditions
# =============================================================================
METRICS: list[dict] = [
    {
        "metric_name": "total_resources",
        "display_name": "Total Resources",
        "description": "Total number of resources in the organization",
        "sql_expression": "COUNT(Resource.ResourceId)",
        "aggregation_type": "count",
        "related_tables": ["Resource"],
        "dimensions": ["BusinessUnitId", "DesignationId", "FunctionId"],
    },
    {
        "metric_name": "active_resources",
        "display_name": "Active Resources",
        "description": "Total number of currently active resources",
        "sql_expression": "SUM(CASE WHEN Resource.IsActive = 1 THEN 1 ELSE 0 END)",
        "aggregation_type": "sum",
        "related_tables": ["Resource"],
        "dimensions": ["BusinessUnitId", "DesignationId"],
    },
    {
        "metric_name": "average_tenure_years",
        "display_name": "Average Tenure (Years)",
        "description": "Average tenure of resources in years",
        "sql_expression": "AVG(Resource.TenureInYears)",
        "aggregation_type": "avg",
        "related_tables": ["Resource"],
    },
    {
        "metric_name": "new_joiners",
        "display_name": "New Joiners",
        "description": "Number of resources who joined in a given period",
        "sql_expression": "COUNT(Resource.ResourceId)",
        "aggregation_type": "count",
        "related_tables": ["Resource"],
        "dimensions": ["DateOfJoin"],
    },
    {
        "metric_name": "reporting_managers_count",
        "display_name": "Reporting Managers Count",
        "description": "Number of resources who are designated as reporting managers",
        "sql_expression": "SUM(CASE WHEN Resource.IsReportingPerson = 1 THEN 1 ELSE 0 END)",
        "aggregation_type": "sum",
        "related_tables": ["Resource"],
    },
    {
        "metric_name": "total_clients",
        "display_name": "Total Clients",
        "description": "Total number of clients in the system",
        "sql_expression": "COUNT(Client.ClientId)",
        "aggregation_type": "count",
        "related_tables": ["Client"],
        "dimensions": ["BusinessUnitId", "DomainId"],
    },
    {
        "metric_name": "active_clients",
        "display_name": "Active Clients",
        "description": "Number of currently active clients",
        "sql_expression": "SUM(CASE WHEN Client.IsActive = 1 THEN 1 ELSE 0 END)",
        "aggregation_type": "sum",
        "related_tables": ["Client"],
    },
    {
        "metric_name": "total_agreement_value",
        "display_name": "Total Agreement Value",
        "description": "Sum of all client agreement values",
        "sql_expression": "SUM(Client.AgreementValue)",
        "aggregation_type": "sum",
        "related_tables": ["Client"],
    },
    {
        "metric_name": "average_hourly_billing_rate",
        "display_name": "Average Hourly Billing Rate",
        "description": "Average hourly billing rate across clients",
        "sql_expression": "AVG(Client.HourlyBillingRate)",
        "aggregation_type": "avg",
        "related_tables": ["Client"],
    },
    {
        "metric_name": "total_business_units",
        "display_name": "Total Business Units",
        "description": "Total number of business units",
        "sql_expression": "COUNT(BusinessUnit.BusinessUnitId)",
        "aggregation_type": "count",
        "related_tables": ["BusinessUnit"],
    },
    {
        "metric_name": "active_business_units",
        "display_name": "Active Business Units",
        "description": "Number of active business units",
        "sql_expression": "SUM(CASE WHEN BusinessUnit.IsActive = 1 THEN 1 ELSE 0 END)",
        "aggregation_type": "sum",
        "related_tables": ["BusinessUnit"],
    },
    {
        "metric_name": "total_client_stakeholders",
        "display_name": "Total Client Stakeholders",
        "description": "Total number of stakeholders across all clients",
        "sql_expression": "COUNT(ClientStakeholder.ClientStakeholderId)",
        "aggregation_type": "count",
        "related_tables": ["ClientStakeholder"],
        "dimensions": ["ClientId"],
    },
    {
        "metric_name": "active_client_stakeholders",
        "display_name": "Active Client Stakeholders",
        "description": "Number of active stakeholders",
        "sql_expression": "SUM(CASE WHEN ClientStakeholder.IsActive = 1 THEN 1 ELSE 0 END)",
        "aggregation_type": "sum",
        "related_tables": ["ClientStakeholder"],
    },
    {
        "metric_name": "total_company_types",
        "display_name": "Total Company Types",
        "description": "Total number of company type classifications",
        "sql_expression": "COUNT(CompanyType.CompanyTypeId)",
        "aggregation_type": "count",
        "related_tables": ["CompanyType"],
    },
    {
        "metric_name": "total_designated_roles",
        "display_name": "Total Designated Roles",
        "description": "Total number of designated roles defined in the system",
        "sql_expression": "COUNT(DesignatedRole.DesignatedRoleId)",
        "aggregation_type": "count",
        "related_tables": ["DesignatedRole"],
    },
     {
        "metric_name": "total_designations",
        "display_name": "Total Designations",
        "description": "Total number of job designations",
        "sql_expression": "COUNT(Designation.DesignationId)",
        "aggregation_type": "count",
        "related_tables": ["Designation"],
    },
    {
        "metric_name": "timesheet_applicable_designations",
        "display_name": "Timesheet Applicable Designations",
        "description": "Number of designations where timesheet logging is required",
        "sql_expression": "SUM(CASE WHEN Designation.IsTimesheetApplies = 1 THEN 1 ELSE 0 END)",
        "aggregation_type": "sum",
        "related_tables": ["Designation"],
    },
    {
        "metric_name": "total_projects",
        "display_name": "Total Projects",
        "description": "Total number of projects",
        "sql_expression": "COUNT(Project.ProjectId)",
        "aggregation_type": "count",
        "related_tables": ["Project"],
        "dimensions": ["ClientId", "BusinessUnitId"],
    },
    {
        "metric_name": "active_projects",
        "display_name": "Active Projects",
        "description": "Number of currently active projects",
        "sql_expression": "SUM(CASE WHEN Project.IsActive = 1 THEN 1 ELSE 0 END)",
        "aggregation_type": "sum",
        "related_tables": ["Project"],
    },
    {
        "metric_name": "average_project_duration",
        "display_name": "Average Project Duration",
        "description": "Average duration of projects",
        "sql_expression": "AVG(CAST(Project.Duration AS FLOAT))",
        "aggregation_type": "avg",
        "related_tables": ["Project"],
    },
    {
        "metric_name": "average_project_billing_rate",
        "display_name": "Average Project Billing Rate",
        "description": "Average hourly billing rate across projects",
        "sql_expression": "AVG(Project.HourlyBillingRate)",
        "aggregation_type": "avg",
        "related_tables": ["Project"],
    },
    {
        "metric_name": "total_allocated_resources",
        "display_name": "Total Allocated Resources",
        "description": "Total number of resource allocations across projects",
        "sql_expression": "COUNT(ProjectResource.ProjectResourceId)",
        "aggregation_type": "count",
        "related_tables": ["ProjectResource"],
        "dimensions": ["ProjectId", "ClientId"],
    },
    {
        "metric_name": "billable_resources",
        "display_name": "Billable Resources",
        "description": "Number of resources assigned as billable",
        "sql_expression": "SUM(CASE WHEN ProjectResource.Billable = 1 THEN 1 ELSE 0 END)",
        "aggregation_type": "sum",
        "related_tables": ["ProjectResource"],
    },
    {
        "metric_name": "average_allocation_percentage",
        "display_name": "Average Allocation Percentage",
        "description": "Average allocation percentage across resources",
        "sql_expression": "AVG(ProjectResource.PercentageAllocation)",
        "aggregation_type": "avg",
        "related_tables": ["ProjectResource"],
    },
    {
        "metric_name": "bench_resources",
        "display_name": "Bench Resources",
        "description": "Number of resources currently on bench",
        "sql_expression": "SUM(CASE WHEN ProjectResource.Bench = 1 THEN 1 ELSE 0 END)",
        "aggregation_type": "sum",
        "related_tables": ["ProjectResource"],
    },
    {
        "metric_name": "active_status_count",
        "display_name": "Active Status Count",
        "description": "Number of active status entries across all categories",
        "sql_expression": "SUM(CASE WHEN Status.IsActive = 1 THEN 1 ELSE 0 END)",
        "aggregation_type": "sum",
        "related_tables": ["Status"],
    },
    {
        "metric_name": "total_technology_categories",
        "display_name": "Total Technology Categories",
        "description": "Total number of defined technology categories",
        "sql_expression": "COUNT(TechCatagory.TechCategoryId)",
        "aggregation_type": "count",
        "related_tables": ["TechCatagory"],
    },
    {
        "metric_name": "active_technology_categories",
        "display_name": "Active Technology Categories",
        "description": "Number of active technology categories",
        "sql_expression": "SUM(CASE WHEN TechCatagory.IsActive = 1 THEN 1 ELSE 0 END)",
        "aggregation_type": "sum",
        "related_tables": ["TechCatagory"],
    },
    {
        "metric_name": "total_technology_functions",
        "display_name": "Total Technology Functions",
        "description": "Total number of technology functions",
        "sql_expression": "COUNT(TechFunction.FunctionId)",
        "aggregation_type": "count",
        "related_tables": ["TechFunction"],
    },
    {
        "metric_name": "active_technology_functions",
        "display_name": "Active Technology Functions",
        "description": "Number of active technology functions",
        "sql_expression": "SUM(CASE WHEN TechFunction.IsActive = 1 THEN 1 ELSE 0 END)",
        "aggregation_type": "sum",
        "related_tables": ["TechFunction"],
    },
    # -------------------------------------------------------------------------
    # TODO: Replace these examples with KPIs from your domain.
    #
    # Example for a sales database:
    # {
    #     "metric_name": "total_revenue",
    #     "display_name": "Total Revenue",
    #     "description": "Sum of all completed order amounts",
    #     "sql_expression": "SUM(orders.amount)",
    #     "aggregation_type": "sum",
    #     "related_tables": ["orders"],
    #     "dimensions": ["region", "product_category", "sales_rep"],
    # },
    # {
    #     "metric_name": "avg_order_value",
    #     "display_name": "Average Order Value",
    #     "description": "Mean value of completed orders",
    #     "sql_expression": "AVG(orders.amount)",
    #     "aggregation_type": "avg",
    #     "related_tables": ["orders"],
    #     "dimensions": ["region", "product_category"],
    # },
    # -------------------------------------------------------------------------
]


# =============================================================================
# 3. DICTIONARY ENTRIES
#    Value-level mappings: what does a coded DB value actually mean?
#    These are scoped to a specific (table_name, column_name) pair.
#
#    The table_name must match exactly the name introspected from your DB.
#    The column must already exist in the schema cache (introspect first).
#
#    Fields per entry:
#      raw_value     (required) The actual stored value, as a string (e.g. "1", "Y", "active")
#      display_value (required) Human-friendly label (e.g. "Active Customer")
#      description   (optional) Longer explanation of what this value means
#      sort_order    (optional) Integer; controls display order (default 0)
# =============================================================================
DICTIONARY_ENTRIES: dict[tuple[str, str], list[dict]] = {
    ("Resource", "IsActive"): [
        {"raw_value": "0", "display_value": "Inactive", "description": "Resource is not active", "sort_order": 1},
        {"raw_value": "1", "display_value": "Active", "description": "Resource is active", "sort_order": 2},
    ],
    ("Resource", "IsReportingPerson"): [
        {"raw_value": "0", "display_value": "Individual Contributor", "description": "Does not manage other resources", "sort_order": 1},
        {"raw_value": "1", "display_value": "Manager", "description": "Manages other resources", "sort_order": 2},
    ],
    ("Client", "IsActive"): [
        {"raw_value": "0", "display_value": "Inactive", "description": "Client is not active", "sort_order": 1},
        {"raw_value": "1", "display_value": "Active", "description": "Client is active", "sort_order": 2},
    ],
    ("Client", "LeaveClauseConfirmed"): [
        {"raw_value": "0", "display_value": "Not Confirmed", "description": "Leave clause not confirmed", "sort_order": 1},
        {"raw_value": "1", "display_value": "Confirmed", "description": "Leave clause confirmed", "sort_order": 2},
    ],
    ("BusinessUnit", "IsActive"): [
        {"raw_value": "0", "display_value": "Inactive", "description": "Business unit is inactive", "sort_order": 1},
        {"raw_value": "1", "display_value": "Active", "description": "Business unit is active", "sort_order": 2},
    ],
    ("ClientStakeholder", "IsActive"): [
        {"raw_value": "0", "display_value": "Inactive", "description": "Stakeholder is inactive", "sort_order": 1},
        {"raw_value": "1", "display_value": "Active", "description": "Stakeholder is active", "sort_order": 2},
    ],
    ("CompanyType", "IsActive"): [
        {"raw_value": "0", "display_value": "Inactive", "description": "Company type is inactive", "sort_order": 1},
        {"raw_value": "1", "display_value": "Active", "description": "Company type is active", "sort_order": 2},
    ],
    ("DesignatedRole", "IsActive"): [
        {"raw_value": "0", "display_value": "Inactive", "description": "Role is inactive", "sort_order": 1},
        {"raw_value": "1", "display_value": "Active", "description": "Role is active", "sort_order": 2},
    ],
    ("Designation", "IsActive"): [
        {"raw_value": "0", "display_value": "Inactive", "description": "Designation is inactive", "sort_order": 1},
        {"raw_value": "1", "display_value": "Active", "description": "Designation is active", "sort_order": 2},
    ],
    ("Designation", "IsTimesheetApplies"): [
        {"raw_value": "0", "display_value": "Not Required", "description": "Timesheet not required", "sort_order": 1},
        {"raw_value": "1", "display_value": "Required", "description": "Timesheet required", "sort_order": 2},
    ],("Project", "IsActive"): [
        {"raw_value": "0", "display_value": "Inactive", "description": "Project is inactive", "sort_order": 1},
        {"raw_value": "1", "display_value": "Active", "description": "Project is active", "sort_order": 2},
    ],
    ("ProjectResource", "Billable"): [
        {"raw_value": "0", "display_value": "Non-Billable", "description": "Work is not billed to client", "sort_order": 1},
        {"raw_value": "1", "display_value": "Billable", "description": "Work is billed to client", "sort_order": 2},
    ],
    ("ProjectResource", "Shadow"): [
        {"raw_value": "0", "display_value": "Primary Resource", "description": "Main assigned resource", "sort_order": 1},
        {"raw_value": "1", "display_value": "Shadow Resource", "description": "Support or backup resource", "sort_order": 2},
    ],
    ("ProjectResource", "Bench"): [
        {"raw_value": "0", "display_value": "Allocated", "description": "Assigned to project", "sort_order": 1},
        {"raw_value": "1", "display_value": "Bench", "description": "Not assigned to billable work", "sort_order": 2},
    ],
    ("ProjectResource", "IsActive"): [
        {"raw_value": "0", "display_value": "Inactive", "description": "Allocation inactive", "sort_order": 1},
        {"raw_value": "1", "display_value": "Active", "description": "Allocation active", "sort_order": 2},
    ],
    ("Status", "ReferenceId"): [
        {"raw_value": "1", "display_value": "Client Status", "description": "Status applicable to clients", "sort_order": 1},
        {"raw_value": "2", "display_value": "Project Status", "description": "Status applicable to projects", "sort_order": 2},
        {"raw_value": "3", "display_value": "Resource Status", "description": "Status applicable to resources", "sort_order": 3},
        {"raw_value": "4", "display_value": "Email Queue Status", "description": "Status for email processing system", "sort_order": 4},
    ],
    ("Status", "IsActive"): [
        {"raw_value": "0", "display_value": "Inactive", "description": "Status is not in use", "sort_order": 1},
        {"raw_value": "1", "display_value": "Active", "description": "Status is currently usable", "sort_order": 2},
    ],
    ("TechCatagory", "IsActive"): [
        {"raw_value": "0", "display_value": "Inactive", "description": "Technology category is inactive", "sort_order": 1},
        {"raw_value": "1", "display_value": "Active", "description": "Technology category is active", "sort_order": 2},
    ],
    ("TechFunction", "IsActive"): [
        {"raw_value": "0", "display_value": "Inactive", "description": "Function is inactive", "sort_order": 1},
        {"raw_value": "1", "display_value": "Active", "description": "Function is active", "sort_order": 2},
    ],

    # -------------------------------------------------------------------------
    # TODO: Replace with coded columns from your schema.
    #
    # Example:
    # ("Orders", "Status"): [
    #     {"raw_value": "0", "display_value": "Pending",   "description": "Order received, not yet processed", "sort_order": 1},
    #     {"raw_value": "1", "display_value": "Processing","description": "Order is being fulfilled",          "sort_order": 2},
    #     {"raw_value": "2", "display_value": "Shipped",   "description": "Dispatched to customer",           "sort_order": 3},
    #     {"raw_value": "3", "display_value": "Delivered", "description": "Confirmed delivered",              "sort_order": 4},
    #     {"raw_value": "4", "display_value": "Cancelled", "description": "Cancelled before fulfilment",      "sort_order": 5},
    # ],
    # ("Customers", "Tier"): [
    #     {"raw_value": "G", "display_value": "Gold",     "description": "Top-tier customer; >$10k LTV", "sort_order": 1},
    #     {"raw_value": "S", "display_value": "Silver",   "description": "Mid-tier customer",            "sort_order": 2},
    #     {"raw_value": "B", "display_value": "Bronze",   "description": "Standard customer",            "sort_order": 3},
    # ],
    # -------------------------------------------------------------------------
}


# =============================================================================
# 4. KNOWLEDGE DOCUMENTS
#    Free-text business context injected into the LLM prompt during queries.
#    Use these for: data model docs, business rules, calculation methodologies,
#    naming conventions, known data quality issues, etc.
#
#    Fields:
#      title   (required) Short descriptive title
#      content (required) The full text of the document (plain text or HTML)
#
#    Tips:
#      - Keep each document focused on one topic (the chunker splits at ~450 words)
#      - HTML is auto-detected and parsed to plain text
#      - You can also import documents via the UI Knowledge tab
# =============================================================================
KNOWLEDGE_DOCS: list[dict] = [
    {
        "title": "PRMS Data Model and Business Semantics Overview",
        "content": """
This system represents a Project Resource Management System (PRMS) designed to manage clients, projects, resources, and their work tracking (EOD/timesheet entries).

CORE ENTITIES:

1. CLIENT
The Client table represents external business clients. Each client is uniquely identified by ClientId.
Clients are associated with Business Units, Organizations, and Functional domains.
Clients include billing configurations such as MonthlyBillingRate, WeeklyBillingRate, and HourlyBillingRate.

A client is considered valid for reporting only when IsActive = 1.

There are two interpretations of client status:
- System Status: Controlled by Client.IsActive (1 = Active, 0 = Inactive)
- Business Status: Derived from Status table via StatusId (e.g., Active, Inactive, Closed)

2. PROJECT
The Project table represents engagements executed for clients.
Each project is linked to:
- Client (ClientId)
- Business Unit (BusinessUnitId)
- Function (FunctionId)

Projects include planned and actual timelines:
- StartDate / EndDate (planned)
- ActualStartDate / ActualEndDate (execution)

Project status is derived from Status table using ProjectStatusId.

3. RESOURCE
The Resource table represents employees or workforce members.
Each resource has:
- EmployeeId (business identifier)
- ResourceName
- Designation, Role, and Reporting hierarchy

A resource is considered active when IsActive = 1.

Resources are associated with:
- Business Units
- Skills (Primaryskill, Secondaryskill)
- Functional roles (FunctionId, DesignatedRoleId)

4. PROJECT RESOURCE (ALLOCATION LAYER)
The ProjectResource table maps resources to projects.

It defines:
- Allocation percentage (PercentageAllocation)
- Billing status (Billable / Non-billable)
- Engagement duration (StartDate, EndDate)
- Billing rates (Rate, HourlyBillingRate)

Special flags:
- Bench = 1 → resource not allocated to active work
- Shadow = 1 → non-primary assignment
- Billable = 1 → revenue-generating allocation

This table is the foundation for utilization and billing calculations.

5. TIMESHEET / EOD (TS_EODDetails)
The TS_EODDetails table captures daily work logs.

Each entry includes:
- Resource (ResourceId, ResourceName)
- Project and task details
- Hours worked (Hrs)
- Completion tracking (Completion, CompletionPercent)

Approval workflow:
- IsApproved = 1 → approved entry
- IsRejected = 1 → rejected entry
- IsDeleted = 1 → logically deleted entry

Only valid work entries should satisfy:
IsApproved = 1 AND IsDeleted = 0 AND IsRejected = 0

These rules must be consistently applied in all reporting metrics.

6. STATUS SYSTEM
The Status table is a shared lookup table used across:
- Client
- Project
- Resource
- Other modules

ReferenceId differentiates status domains:
- 1 → Client Status
- 2 → Project Status
- 3 → Resource Status
- 4 → Email Queue Status

Important:
The same StatusName (e.g., "Active") can exist in multiple domains.
Therefore, status interpretation must always consider ReferenceId.

7. ORGANIZATIONAL STRUCTURE

- BusinessUnit → logical grouping of operations
- Organization → higher-level grouping
- TechFunction → functional domain (e.g., engineering, analytics)

Entities like Client, Project, and Resource are linked to these structures.

8. LOCATION DATA

Geographic information is stored using:
- countries (countryID)
- cities (cityID)

These are reference tables used for address mapping.

9. CLASSIFICATION TABLES

Several tables act as lookup/reference systems:
- CategoryTable / CategoryType
- CompanyType
- Domain
- Designation / DesignatedRole
- TechCategory / TechFunction

These define classification hierarchies used across the system.

10. BILLING MODEL

Billing is defined at multiple levels:
- Client level (default rates)
- Project level (override rates)
- ProjectResource level (final billing rate)

Hierarchy:
ProjectResource > Project > Client

Billing calculations must respect this override order.

11. DATA VALIDITY RULES (CRITICAL)

The following filters are considered standard across reporting:

- Active records:
  IsActive = 1

- Valid EOD entries:
  IsApproved = 1 AND IsDeleted = 0 AND IsRejected = 0

- Active allocations:
  ProjectResource.IsActive = 1

These conditions must always be applied when computing metrics.

12. RELATIONSHIP SUMMARY

- Client → Project (1 to many)
- Project → ProjectResource (1 to many)
- Resource → ProjectResource (1 to many)
- Resource → TS_EODDetails (1 to many)
- Client → BusinessUnit / Organization / Function (many to one)

13. REPORTING PRINCIPLES

- Always filter inactive records unless explicitly required
- Avoid mixing system status (IsActive) with business status (StatusId)
- Prefer ProjectResource for allocation and billing analysis
- Prefer TS_EODDetails for work tracking and effort metrics

14. SEMANTIC LAYER USAGE

This knowledge document is used to:
- provide business context to the LLM
- ensure correct interpretation of metrics and glossary terms
- enforce consistent filtering and relationships

All metrics and glossary definitions must align with these rules.

STRICT SCHEMA ENFORCEMENT:

Only use tables and columns that exist in the database schema.

Do NOT generate or assume:
- Table names not present in the schema
- Columns not present in the schema

Examples of INVALID behavior:
- Using table 'EMP' if it does not exist
- Using column 'EmployeeName' if it does not exist

If the required data is not available in known tables, return no result instead of guessing.

QUERY SIMPLICITY RULE:

Always generate the simplest possible query that answers the question.

Avoid:
- unnecessary joins
- subqueries
- extra tables

Only include tables that are strictly required.

NO GUESSING RULE:

If a user query references an entity that cannot be mapped to a known table or column, do not infer or guess.

Instead:
- Use closest matching known table
OR
- Return that data is not available

Never invent schema elements.
        """.strip()
    }

    # -------------------------------------------------------------------------
    # TODO: Add your own documents. Examples below.
    #
    # {
    #     "title": "Data Model Overview",
    #     "content": """
    # Our database contains three main areas:
    #
    # CUSTOMERS: The Customers table holds all registered users. The primary key is
    # CustomerId (integer). CustomerTier (G/S/B) indicates loyalty level.
    #
    # ORDERS: Each row is one order. Status is an integer code (0=Pending,
    # 1=Processing, 2=Shipped, 3=Delivered, 4=Cancelled). OrderDate is UTC.
    # Amount is in USD, stored as decimal(18,2).
    #
    # PRODUCTS: ProductId links to Orders.ProductId. Category and SubCategory
    # are free-text strings. IsActive = 1 means the product is currently sold.
    #     """.strip(),
    # },
    # {
    #     "title": "Revenue Recognition Rules",
    #     "content": """
    # Revenue is recognised when Status = 3 (Delivered). Cancelled orders (Status=4)
    # must never be included in revenue figures. Refunded orders are recorded as
    # negative-amount rows with Status = 3.
    #
    # For month-on-month comparisons, always filter on MONTH(OrderDate) and
    # YEAR(OrderDate) — do not use CreatedDate.
    #
    # Tax is not included in the Amount column. The TaxAmount column is separate.
    # Net Revenue = Amount. Gross Revenue = Amount + TaxAmount.
    #     """.strip(),
    # },
    # -------------------------------------------------------------------------
]


# =============================================================================
# Seeding functions — no need to edit below this line
# =============================================================================

def find_connection(client: httpx.Client, connection_name: str | None) -> dict:
    """Find the target connection — by name if specified, else first sqlserver."""
    resp = client.get(f"{API_PREFIX}/connections")
    resp.raise_for_status()
    connections = resp.json()

    if not connections:
        print("ERROR: No connections found. Create and introspect a connection first.")
        sys.exit(1)

    if connection_name:
        match = next((c for c in connections if c["name"] == connection_name), None)
        if not match:
            names = [c["name"] for c in connections]
            print(f"ERROR: Connection '{connection_name}' not found. Available: {names}")
            sys.exit(1)
        return match

    # Default: prefer first sqlserver connection, fallback to first of any type
    sqlserver = next((c for c in connections if c["connector_type"] == "sqlserver"), None)
    target = sqlserver or connections[0]
    print(f"  Using connection: {target['name']} ({target['id']}, {target['connector_type']})")
    return target


def seed_glossary(client: httpx.Client, connection_id: str) -> None:
    if not GLOSSARY_TERMS:
        print("\n--- Glossary: nothing to seed (GLOSSARY_TERMS is empty) ---")
        return

    print(f"\n--- Seeding {len(GLOSSARY_TERMS)} Glossary Terms ---")
    ok = fail = 0
    for term_data in GLOSSARY_TERMS:
        resp = client.post(f"{API_PREFIX}/connections/{connection_id}/glossary", json=term_data)
        if resp.status_code == 201:
            print(f"  + {term_data['term']}")
            ok += 1
        else:
            print(f"  ! {term_data['term']} — HTTP {resp.status_code}: {resp.text[:120]}")
            fail += 1
    print(f"  Glossary: {ok} created, {fail} failed")


def seed_metrics(client: httpx.Client, connection_id: str) -> None:
    if not METRICS:
        print("\n--- Metrics: nothing to seed (METRICS is empty) ---")
        return

    print(f"\n--- Seeding {len(METRICS)} Metrics ---")
    ok = fail = 0
    for metric_data in METRICS:
        resp = client.post(f"{API_PREFIX}/connections/{connection_id}/metrics", json=metric_data)
        if resp.status_code == 201:
            print(f"  + {metric_data['display_name']}")
            ok += 1
        else:
            print(f"  ! {metric_data['display_name']} — HTTP {resp.status_code}: {resp.text[:120]}")
            fail += 1
    print(f"  Metrics: {ok} created, {fail} failed")


def seed_dictionary(client: httpx.Client, connection_id: str) -> None:
    if not DICTIONARY_ENTRIES:
        print("\n--- Dictionary: nothing to seed (DICTIONARY_ENTRIES is empty) ---")
        return

    print(f"\n--- Seeding Dictionary Entries for {len(DICTIONARY_ENTRIES)} columns ---")

    # Build (table_name, column_name) → column_id map from the cached schema
    tables_resp = client.get(f"{API_PREFIX}/connections/{connection_id}/tables")
    tables_resp.raise_for_status()
    tables = tables_resp.json()

    column_map: dict[tuple[str, str], str] = {}
    print(f"  Scanning {len(tables)} tables for column IDs...")
    for table_summary in tables:
        detail_resp = client.get(f"{API_PREFIX}/tables/{table_summary['id']}")
        detail_resp.raise_for_status()
        detail = detail_resp.json()
        for col in detail["columns"]:
            column_map[(detail["table_name"], col["column_name"])] = col["id"]

    ok = fail = skip = 0
    for (table_name, column_name), entries in DICTIONARY_ENTRIES.items():
        col_id = column_map.get((table_name, column_name))
        if not col_id:
            print(f"  ! {table_name}.{column_name} — not found in schema cache (introspect first?)")
            skip += 1
            continue

        print(f"  {table_name}.{column_name} ({len(entries)} entries):")
        for entry_data in entries:
            resp = client.post(f"{API_PREFIX}/columns/{col_id}/dictionary", json=entry_data)
            if resp.status_code == 201:
                print(f"    + {entry_data['raw_value']!r} → {entry_data['display_value']}")
                ok += 1
            else:
                print(f"    ! {entry_data['raw_value']!r} — HTTP {resp.status_code}: {resp.text[:100]}")
                fail += 1

    print(f"  Dictionary: {ok} created, {fail} failed, {skip} columns skipped")


def seed_knowledge(client: httpx.Client, connection_id: str) -> None:
    if not KNOWLEDGE_DOCS:
        print("\n--- Knowledge: nothing to seed (KNOWLEDGE_DOCS is empty) ---")
        return

    print(f"\n--- Seeding {len(KNOWLEDGE_DOCS)} Knowledge Documents ---")
    ok = fail = 0
    for doc in KNOWLEDGE_DOCS:
        payload = {
            "title": doc["title"],
            "content": doc["content"],
            "source_url": None,
        }
        resp = client.post(
            f"{API_PREFIX}/connections/{connection_id}/knowledge",
            json=payload,
        )
        if resp.status_code == 201:
            result = resp.json()
            print(f"  + {doc['title']} ({result.get('chunk_count', '?')} chunks)")
            ok += 1
        else:
            print(f"  ! {doc['title']} — HTTP {resp.status_code}: {resp.text[:120]}")
            fail += 1
    print(f"  Knowledge: {ok} created, {fail} failed")


def print_schema_summary(client: httpx.Client, connection_id: str) -> None:
    """Print all table/column names to help fill in DICTIONARY_ENTRIES."""
    print("\n--- Schema Summary (use this to fill in DICTIONARY_ENTRIES) ---")
    tables_resp = client.get(f"{API_PREFIX}/connections/{connection_id}/tables")
    tables_resp.raise_for_status()
    tables = tables_resp.json()

    for table_summary in tables:
        detail_resp = client.get(f"{API_PREFIX}/tables/{table_summary['id']}")
        detail_resp.raise_for_status()
        detail = detail_resp.json()
        cols = [c["column_name"] for c in detail["columns"]]
        print(f"  {detail['table_name']}: {', '.join(cols)}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Seed semantic metadata for an Azure SQL Server connection into QueryWise"
    )
    parser.add_argument(
        "--base-url",
        default="http://localhost:8000",
        help="QueryWise backend URL (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--connection-name",
        default=None,
        help="Name of the connection to seed (default: first sqlserver connection)",
    )
    parser.add_argument(
        "--schema-only",
        action="store_true",
        help="Only print the schema summary — useful for discovering column names before filling in the seed data",
    )
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    print(f"QueryWise SQL Server Metadata Seeder → {base_url}")

    with httpx.Client(base_url=base_url, timeout=60) as client:
        # Verify backend is reachable
        try:
            client.get(f"{API_PREFIX}/health").raise_for_status()
        except (httpx.ConnectError, httpx.HTTPStatusError) as e:
            print(f"ERROR: Cannot reach backend at {base_url}: {e}")
            sys.exit(1)

        conn = find_connection(client, args.connection_name)
        connection_id = conn["id"]

        if args.schema_only:
            print_schema_summary(client, connection_id)
            return

        seed_glossary(client, connection_id)
        seed_metrics(client, connection_id)
        seed_dictionary(client, connection_id)
        seed_knowledge(client, connection_id)

    print("\n✓ Done. Verify in the UI:")
    print(f"  Glossary    → http://localhost:5173/glossary")
    print(f"  Metrics     → http://localhost:5173/metrics")
    print(f"  Dictionary  → Connections → click a table → Dictionary tab")
    print(f"  Knowledge   → http://localhost:5173/knowledge")
    print(f"\nEmbeddings will generate in the background — check the progress")
    print(f"banner in the UI or: GET {base_url}/api/v1/embeddings/status")


if __name__ == "__main__":
    main()
