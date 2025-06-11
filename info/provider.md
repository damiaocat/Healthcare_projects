# Provider Ingestion Knowledge Transfer Document

**Document Owner:** [Your Name]  
**Last Updated:** June 10, 2025  
**Transition Period:** June 5-18, 2025

## Overview

This document covers the complete provider ingestion process, including automated and manual workflows, configuration management, troubleshooting, and collaboration with the EDM team.

## 1. Provider Ingestion Scope

### Current Responsibilities
- **Total Providers:** 9 providers + 1 vendor (10 total data sources), more to come in the future
- **Automation Status:** Mixed (some automated, some require manual intervention)
- **Manual Triggers:** Required when Charuta calls out for specific refreshes
- **Primary Systems:** EDM Collaboration Group, Tidal, Airflow

## 2. Provider Ingestion Process

### 2.1 Automated Ingestions

**Process Flow:**
1. Scheduled jobs run automatically via Tidal
2. Monitor job completion through standard monitoring email alerts
3. Verify data quality through audit DAGs

**Key Monitoring Points:**
- Job completion status in Tidal
- Data volume validation
- Error logs and alerts
- Audit DAG results
- Daily monitoring emails

### 2.2 Manual Ingestions

**Trigger Scenarios:**
- Charuta callouts for specific provider refreshes
- Failed automated jobs requiring manual intervention
- Ad-hoc data requests
- Configuration updates or testing

**Manual Process Steps:**
1. Receive notification/request from Charuta or team
2. Identify the specific provider and configuration
3. Access EDM Collaboration Group for configuration details
4. Execute manual run
5. Monitor completion and validate results including if any data shifting or missing records. 
6. Update stakeholders on completion status

## 3. Configuration Management

### 3.1 EDM Collaboration Group
**Location:** [Specify exact path/location]
**Contents:**
- File ingestion configurations for all providers

**Access Process:**
1. Navigate to EDM Collaboration Group
2. Find the job name related to the provider

### 3.2 Tidal Scheduling
**Location:** [Specify Tidal environment/path]
**Contents:**
- Scheduled job definitions
- Dependency mappings
- Notification configurations

**Key Operations:**
- View job schedules and status
- Modify scheduling parameters
- Handle job dependencies
- Monitor resource utilization

## 4. Common Issues and Troubleshooting

### 4.1 Frequent Issues Encountered

**Data Quality Issues:**

*Missing Value Problems:*
- **Symptoms:** Empty fields, NULL values, 'N/A' strings, inconsistent null representations
- **Resolution:** Check source data quality, validate null handling logic, coordinate with data providers
- **Prevention:** Implement consistent null value standards, regular data profiling

*Data Type Issues:*
- **Symptoms:** Integers as strings, floats truncated to integers, type mismatches
- **Examples:** '123.0' instead of 123, quoted numbers, 'NaN' strings for numeric fields
- **Resolution:** Review data type mappings, validate transformation logic, update parsing rules

*Whitespace and Formatting Issues:*
- **Symptoms:** Leading/trailing spaces, tab characters, unexpected line breaks
- **Examples:** '  value  ', '\tvalue\n', fields with embedded newlines
- **Resolution:** Implement data trimming, standardize whitespace handling

*Special Character Problems:*
- **Symptoms:** Unexpected symbols in data, encoding issues, delimiter conflicts
- **Examples:** '@', '#', '

### 4.2 Troubleshooting Workflow
1. **Identify the Issue:** Review error logs, monitoring alerts, stakeholder reports
2. **Assess Impact:** Determine affected providers, data sets, and downstream processes
3. **Initial Diagnosis:** Check configurations, job status
4. **Resolution Attempt:** Apply standard fixes, restart jobs, validate configurations

*Date Format Issues:*
- **Symptoms:** Inconsistent date formats, invalid date strings, timezone problems
- **Examples:** MM/DD/YYYY vs DD/MM/YYYY vs YYYY/MM/DD, 'invalid_date' strings
- **Resolution:** Standardize date parsing, validate date formats, implement date transformation rules

**CSV File Structure Issues:**

*Column Count Mismatches:*
- **Symptoms:** Extra columns, missing columns, variable row lengths
- **Examples:** Files with additional unexpected columns or missing expected fields
- **Resolution:** Validate file structure before processing, implement column mapping flexibility

*Quote and Delimiter Problems:*
- **Symptoms:** Malformed CSV structure, broken field boundaries, escaped quotes
- **Examples:** Problematic quotes like '"field""extra"', semicolons instead of commas
- **Resolution:** Review CSV parsing parameters, validate delimiter consistency, fix quote escaping

*Line Break Issues:*
- **Symptoms:** Unexpected line breaks within fields, broken record structure
- **Examples:** Multi-line text fields breaking CSV parsing
- **Resolution:** Implement proper CSV parsing with quote handling, validate record integrity

**Connection and System Issues:**
- **Symptoms:** Authentication failures, timeout errors, network connectivity
- **Resolution:** Verify credentials, check network status, coordinate with infrastructure team
- **Escalation:** Contact EDM team for credential resets

**Job Scheduling Issues:**
- **Symptoms:** Jobs not triggering, dependency failures, resource conflicts
- **Resolution:** Review Tidal configurations, check dependencies, validate resource availability
- **Escalation:** Coordinate with scheduling team

**Configuration Errors:**
- **Symptoms:** Job failures, incorrect data mapping, transformation errors
- **Resolution:** Review configuration files, validate against requirements, test changes
- **Escalation:** Work with EDM team for complex configuration issues

## 5. EDM Team Collaboration

### 5.1 When to Contact EDM Team

**Configuration Issues:**
- Complex configuration changes required
- System integration problems

**Technical Issues:**
- Infrastructure-related problems
- Credential management issues
- System performance concerns
- unexpected functionality

## 6. Audit Process

### 6.1 Audit DAGs Configuration

**Primary Audit DAGs:**
- `rdbms_audit`: Database-level auditing, no archival;
- `provider_audit`: Provider-specific data validation, with archival;
- `provider_audit_manual`: Manual audit processes, no consumption script refresh


## 7. Monitoring and Alerting

- Review overnight job completions
- Check data volume trends
- Validate audit results
- Respond to alerts and notifications

## 8. Handover Checklist

### Access and Permissions
- [ ] EDM Collaboration Group access verified
- [ ] Tidal system access confirmed
- [ ] Airflow DAG permissions validated
- [ ] Emergency contact information shared

### Knowledge Transfer
- [ ] Provider-specific configurations reviewed
- [ ] Manual process procedures demonstrated
- [ ] Troubleshooting scenarios practiced
- [ ] EDM communication process explained

### Documentation
- [ ] Configuration spreadsheet completed
- [ ] Issue log and resolution history transferred
- [ ] Contact lists updated
- [ ] Process improvements documented


*Date Format Issues:*
- **Symptoms:** Inconsistent date formats, invalid date strings, timezone problems
- **Examples:** MM/DD/YYYY vs DD/MM/YYYY vs YYYY/MM/DD, 'invalid_date' strings
- **Resolution:** Standardize date parsing, validate date formats, implement date transformation rules

**CSV File Structure Issues:**

*Column Count Mismatches:*
- **Symptoms:** Extra columns, missing columns, variable row lengths
- **Examples:** Files with additional unexpected columns or missing expected fields
- **Resolution:** Validate file structure before processing, implement column mapping flexibility

*Quote and Delimiter Problems:*
- **Symptoms:** Malformed CSV structure, broken field boundaries, escaped quotes
- **Examples:** Problematic quotes like '"field""extra"', semicolons instead of commas
- **Resolution:** Review CSV parsing parameters, validate delimiter consistency, fix quote escaping

*Line Break Issues:*
- **Symptoms:** Unexpected line breaks within fields, broken record structure
- **Examples:** Multi-line text fields breaking CSV parsing
- **Resolution:** Implement proper CSV parsing with quote handling, validate record integrity

**Connection and System Issues:**
- **Symptoms:** Authentication failures, timeout errors, network connectivity
- **Resolution:** Verify credentials, check network status, coordinate with infrastructure team
- **Escalation:** Contact EDM team for credential resets

**Job Scheduling Issues:**
- **Symptoms:** Jobs not triggering, dependency failures, resource conflicts
- **Resolution:** Review Tidal configurations, check dependencies, validate resource availability
- **Escalation:** Coordinate with scheduling team

**Configuration Errors:**
- **Symptoms:** Job failures, incorrect data mapping, transformation errors
- **Resolution:** Review configuration files, validate against requirements, test changes
- **Escalation:** Work with EDM team for complex configuration issues

### 4.2 Troubleshooting Workflow
1. **Identify the Issue:** Review error logs, monitoring alerts, stakeholder reports
2. **Assess Impact:** Determine affected providers, data sets, and downstream processes
3. **Initial Diagnosis:** Check configurations, job status, system health
4. **Resolution Attempt:** Apply standard fixes, restart jobs, validate configurations
5. **Escalation:** Contact appropriate teams if initial resolution fails
6. **Documentation:** Update issue log, communicate resolution to stakeholders

## 5. EDM Team Collaboration

### 5.1 When to Contact EDM Team

**Configuration Issues:**
- Complex configuration changes required
- New provider onboarding
- Data mapping modifications
- System integration problems

**Technical Issues:**
- Infrastructure-related problems
- Credential management issues
- System performance concerns
- Integration failures

### 5.2 EDM Communication Process

**Primary Contacts:**
- [List key EDM team contacts with roles and contact information]

**Communication Channels:**
- Email: [EDM team email]
- Slack: [EDM team channel]
- Service Desk: [Ticket system information]

**Information to Provide:**
- Provider name and affected systems
- Error messages and logs
- Timeline and business impact
- Steps already attempted
- Urgency level

**Escalation Path:**
1. **Level 1:** Direct contact with EDM team member
2. **Level 2:** EDM team lead escalation
3. **Level 3:** Management escalation for critical issues

## 6. Audit Process

### 6.1 Audit DAGs Configuration

**Primary Audit DAGs:**
- `rdbms_audit`: Database-level auditing
- `provider_audit`: Provider-specific data validation
- `provider_audit_manual`: Manual audit processes

**Audit Frequency:**
- Daily automated audits
- Manual audits as needed
- Post-ingestion validation

### 6.2 Audit Process Steps
1. **Pre-Audit Setup:** Ensure audit configurations are current
2. **Execution:** Run appropriate audit DAG
3. **Results Review:** Analyze audit output for anomalies
4. **Issue Resolution:** Address any identified problems
5. **Documentation:** Record audit results and actions taken

### 6.3 Which DAG to Use
- **rdbms_audit:** For database connectivity and structure validation
- **provider_audit:** For standard provider data validation
- **provider_audit_manual:** For custom validation scenarios or troubleshooting

## 7. Monitoring and Alerting

### 7.1 Daily Monitoring Tasks
- Review overnight job completions
- Check data volume trends
- Validate audit results
- Respond to alerts and notifications

### 7.2 Key Metrics to Monitor
- Job success/failure rates
- Data volume comparisons
- Processing time trends
- Error frequency and types

## 8. Emergency Procedures

### 8.1 Critical Issue Response
1. **Immediate Assessment:** Determine scope and business impact
2. **Stakeholder Notification:** Alert relevant teams and management
3. **Resolution Focus:** Apply emergency fixes or workarounds
4. **Communication:** Provide regular updates on resolution progress
5. **Post-Incident:** Conduct review and implement preventive measures

### 8.2 Emergency Contacts
- **Primary Escalation:** [Manager/Team Lead contact]
- **EDM Emergency:** [EDM team emergency contact]
- **Business Stakeholders:** [Key business contacts]

## 9. Handover Checklist

### Access and Permissions
- [ ] EDM Collaboration Group access verified
- [ ] Tidal system access confirmed
- [ ] Airflow DAG permissions validated
- [ ] Emergency contact information shared

### Knowledge Transfer
- [ ] Provider-specific configurations reviewed
- [ ] Manual process procedures demonstrated
- [ ] Troubleshooting scenarios practiced
- [ ] EDM communication process explained

### Documentation
- [ ] Configuration spreadsheet completed
- [ ] Issue log and resolution history transferred
- [ ] Contact lists updated
- [ ] Process improvements documented
