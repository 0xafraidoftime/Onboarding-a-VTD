# MBAM Secondary VTD Limit Onboarding

## Overview

This project implements limit onboarding for MBAM (Municipal Banking and Markets) Secondary VTD (Volcker Trading Desk) within the CFTC (Commodity Futures Trading Commission) risk management system. The implementation ensures proper breach and no-breach alert generation for new limit codes.

## Project Details

**JIRA Ticket**: [FICCRISK-32258] Limit Onboarding for MBAM secondary VTD  
**Link**: [Horizon/Jira2](https://jira2.horizon.bankofamerica.com/browse/FICCRISK-32258)

## Test Scenario

### Given
- Exposures exist in BRISK for MBAM SECONDARY
- New limit code created in LMS (Limit Management System)

### When
- VTD CFTC breach calculation job is executed in UAT

### Then
1. Breach and No Breach emails are generated successfully
2. BRISK report and exposures in email match
3. Workflow executes successfully
4. End of day reports, control reports, and monthly reports work successfully

## Limit Configuration

### MBAM Secondary Limit Details
- **Volcker Business Area**: MUNICIPAL BANKING AND MARKETS
- **Volcker Trading Desk**: MBAM SECONDARY
- **Limit Code**: INTRA_FICC_237
- **Limit Name**: INTRA_FICC_MUNI SECONDARY_USD_CS01+1bp
- **Limit Value**: 2,000,000.000 USD
- **Currency**: USD
- **Status**: Active
- **Risk Factor**: Credit Spread
- **Measure**: CS01
- **Measure Unit**: b.p.
- **Entity ID**: 8009245
- **Entity Name**: MUNICIPAL BANKING AND MARKETS

## Technical Implementation

### Core Components

#### 1. CFTCLimits Class (`cftclimits.py`)
Main class responsible for:
- Fetching limit data from LMS API
- Storing data in Sandra database
- Managing limit snapshots and comparisons
- Sending email notifications for exceptions and changes

#### 2. Key Features
- **API Integration**: Connects to LMS API to fetch real-time limit data
- **Data Storage**: Uses Sandra database for persistent storage
- **Change Detection**: Compares current and previous snapshots to identify modifications
- **Email Notifications**: Sends alerts for limit changes, exceptions, and breaches
- **Schema Mapping**: Converts between LMS and internal data schemas

### Configuration Parameters
```python
SCHEMANAME = 'limitsapi'
DIRECTORY = '/Risk/Market/Mrq/Domain'

# Column schemas for limit data
_LIMIT_COL_NAME = [
    'Volcker Business Area', 'Volcker Trading Desk', 'Limit Code', 
    'Limit Name', 'Limit Value', 'Currency', 'Status', 'Maturity Name',
    'Product Type', 'Risk Factor Name', 'Significant Rating', 'Shift Name',
    'Measure', 'Measure Unit', 'entity_id', 'entity_name'
]
```

## Setup and Deployment

### Prerequisites
- Access to UAT environment
- LMS API connectivity
- Sandra database access
- Proper authentication (Kerberos/PKI)

### Environment Configuration
The system supports both Windows (Kerberos) and non-Windows (PKI) authentication:
- **Windows**: Uses Kerberos authentication
- **Non-Windows**: Uses PKI authentication

### Running the System
```python
# Entry point
def run(config='uat_common'):
    cftc = CFTCLimits(config)
    cftc.start()
```

## Monitoring and Reporting

### Generated Reports
1. **Breach Reports**: Identify when limits are exceeded
2. **No-Breach Reports**: Confirm limits are within bounds
3. **Comparison Reports**: Show changes between snapshots
4. **End-of-Day Reports**: Daily summary of limit status
5. **Control Reports**: Audit trail and control information
6. **Monthly Reports**: Aggregated monthly statistics

### Email Notifications
- **Exception Emails**: Sent when API failures occur
- **Limit Change Emails**: Sent when limits are modified, added, or removed
- **Breach Alerts**: Sent when limits are exceeded

## Data Flow

1. **Data Fetch**: System retrieves limit data from LMS API
2. **Data Processing**: Maps data to internal schema and applies any manual updates
3. **Comparison**: Compares with previous snapshot to detect changes
4. **Storage**: Saves processed data to Sandra database
5. **Notification**: Sends appropriate emails based on changes detected
6. **Reporting**: Generates various reports for monitoring and compliance

## Validation Steps

### Pre-Deployment Checklist
- [ ] Limit code created in LMS UAT
- [ ] Exposures configured in BRISK
- [ ] Measure details validated with LOB (Line of Business)
- [ ] BRISK report created with proper filters

### Post-Deployment Verification
- [ ] Breach/No-breach emails generated
- [ ] BRISK report data matches email content
- [ ] Workflow completes successfully
- [ ] All reports generate without errors
- [ ] Limit changes reflect in comparison reports

## Troubleshooting

### Common Issues
1. **API Connectivity**: Ensure proper authentication and network access
2. **Schema Mismatches**: Verify column mapping between LMS and internal schemas
3. **Missing Data**: Check if limit codes exist in LMS before processing
4. **Email Failures**: Validate SMTP configuration and recipient lists

### Logs and Monitoring
- Check application logs for detailed error messages
- Monitor Sandra database for data integrity
- Verify email delivery status
- Review BRISK reports for data accuracy

## Support and Contacts

For technical issues or questions regarding MBAM Secondary VTD limit onboarding:
- Review JIRA ticket FICCRISK-32258 for detailed requirements
- Contact the Line of Business for measure details and validation
- Check system logs and Sandra database for troubleshooting

---

**Last Updated**: Based on implementation as of July 2025  
**Version**: 1.18  
**Environment**: UAT/Production Ready
