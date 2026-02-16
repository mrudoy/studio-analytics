# Union.fit Report Map

> Complete catalog of all reports available in the Union.fit admin panel for SKY TING.
> Base URL: `https://www.union.fit/admin/orgs/sky-ting`
> Last updated: 2026-02-16

## Navigation Structure

Top admin nav: Public | **Dashboard** | Products > | Pricing > | People > | Marketing > | **Reports >**

---

## Reports Menu (14 categories)

### 1. Business Health
| Report | URL Path | Data |
|---|---|---|
| Dashboard | `/dashboard` | Revenue, Visits, Subscriptions, New Customers, Feedback, Support (4-week charts) |
| Profitability | TBD | |
| Performances | TBD | |

### 2. Events
| Report | URL Path | Data |
|---|---|---|
| All Performances | TBD | |
| Performances by Teacher | TBD | |
| Event Summaries | TBD | |
| Series Attendance | TBD | |

### 3. Courses
| Report | URL Path | Data |
|---|---|---|
| Course Attendance | TBD | |

### 4. Replays
| Report | URL Path | Data |
|---|---|---|
| All Replays | TBD | |
| Replays by Teacher | TBD | |

### 5. Payroll
| Report | URL Path | Data |
|---|---|---|
| Reports | TBD | Teacher pay reports |
| Rules | TBD | Pay rules config |
| Categories | TBD | Pay categories |

### 6. Customers
| Report | URL Path | Data |
|---|---|---|
| **New Customers** | `/report/customers/created_within` | Name, email, role, orders, created date |
| Unique Attendees | TBD | |
| Support Responses | TBD | |
| VaxGuard Approved | TBD | |

### 7. Balances
| Report | URL Path | Data |
|---|---|---|
| Summary | TBD | |
| Transactions | TBD | |

### 8. Registrations
| Report | URL Path | Data |
|---|---|---|
| All | TBD | |
| Average Revenue/Visit | TBD | Revenue per visit metrics |
| **First Visit** | `/report/registrations/first_visit` | Attendee, performance, type, redeemedAt, pass, status |
| Stats | TBD | |
| No Shows | TBD | |
| **Remaining** | `/report/registrations/remaining` | Customer, pass, remaining, total, expires, price, lastTeacher, revenueCategory |
| Waitlist | TBD | |

### 9. Sales (THE BIG ONE - all revenue data lives here)
| Report | URL Path | Data |
|---|---|---|
| **Sales by Revenue Category** | `/reports/revenue` | **REVENUE CATEGORY, REVENUE, UNION FEES, STRIPE FEES, TRANSFERS, REFUNDED, UNION FEES REFUNDED, NET REVENUE** — filterable by Cash/All, date range. Has "Details" drill-down per category. This is the master revenue report. |
| Sales by Service | TBD | |
| Sales by Payment Method | TBD | |
| Sales by Seller | TBD | |
| Sales by Channel | TBD | |
| Discounts | TBD | |
| Revenue by Location | TBD | Revenue split by studio location |
| Payment Plans | TBD | |
| Payouts | TBD | Stripe payout records |
| **Orders** | `/reports/transactions?transaction_type=orders` | Created, code, customer, type, payment, total |
| Refunds | TBD | |
| Transfers | TBD | |
| Taxes Collected | TBD | |

### 10. Retail
| Report | URL Path | Data |
|---|---|---|
| Inventory | TBD | |
| Inventory Summary | TBD | |
| Cost of Goods Sold | TBD | |
| Taxes Collected | TBD | |

### 11. Auto-Renews (subscription management)
| Report | URL Path | Data |
|---|---|---|
| Summary | TBD | |
| **Recurring Revenue** | `/report/subscriptions/projected_revenue` | Billed, Upcoming, Total — daily bar chart (Billed Subscriptions, Upcoming Subscriptions, Billed Payment Plans). Month selector. Shows projected + actual recurring revenue. |
| Net New | `/report/subscriptions/growth` (default filter) | Chart + table of net new subs over time. Columns: NAME, STATE, PRICE, CUSTOMER, CREATED, CANCELED AT |
| **New** | `/report/subscriptions/growth?filter=new` | New subscriptions. Same columns as Net New. |
| **Cancelled** | `/report/subscriptions/growth?filter=cancelled` | Canceled subscriptions with canceledAt date. |
| **Active** | `/report/subscriptions/list?status=active` | All active subscriptions. Name, state, price, customer, email, created. |
| Past Due | `/report/subscriptions/list?status=past_due` (assumed) | |
| **Paused** | `/report/subscriptions/list?status=paused` | Paused subscriptions. |
| Retention | TBD | Retention/churn metrics |
| Multiple | TBD | Customers with multiple subscriptions |

### 12. Feedback
| Report | URL Path | Data |
|---|---|---|
| Feedback | (single page, no submenu) | Customer feedback/ratings |

### 13. Video
| Report | URL Path | Data |
|---|---|---|
| Delivery | TBD | Video delivery metrics |
| Favorites | TBD | Most favorited videos |
| Usage | TBD | Video usage/watch time |
| Views | TBD | Video view counts |
| Audit | TBD | Video audit trail |

### 14. Fitreport
| Report | URL Path | Data |
|---|---|---|
| Fitreport | (single page, no submenu) | Third-party fitness report integration |

---

## Currently Scraped Reports (8)

| Report | Key | URL Path | Download Strategy |
|---|---|---|---|
| New Customers | `newCustomers` | `/report/customers/created_within` | HTML scrape |
| Orders | `orders` | `/reports/transactions?transaction_type=orders` | HTML scrape |
| First Visits | `firstVisits` | `/report/registrations/first_visit` | HTML scrape |
| All Registrations | `allRegistrations` | `/report/registrations/remaining` | HTML scrape |
| Active Auto-Renews | `activeAutoRenews` | `/report/subscriptions/list?status=active` | CSV fetch |
| Paused Auto-Renews | `pausedAutoRenews` | `/report/subscriptions/list?status=paused` | CSV fetch |
| Canceled Auto-Renews | `canceledAutoRenews` | `/report/subscriptions/growth?filter=cancelled` | CSV fetch |
| New Auto-Renews | `newAutoRenews` | `/report/subscriptions/growth?filter=new` | CSV fetch |

## Priority Reports to Add

### 1. Revenue Categories (HIGH PRIORITY)
- **URL**: `/reports/revenue`
- **Why**: Contains ALL revenue by category including drop-in $, workshop $, subscription $, etc. This is the single source of truth for total business revenue.
- **Columns**: REVENUE CATEGORY, REVENUE, UNION FEES, STRIPE FEES, TRANSFERS, REFUNDED, UNION FEES REFUNDED, NET REVENUE
- **Filters**: Cash/All dropdown, date range picker
- **Download**: HTML table scrape (has "View" dropdown but unclear if CSV export available)

### 2. Recurring Revenue (MEDIUM PRIORITY)
- **URL**: `/report/subscriptions/projected_revenue`
- **Why**: Shows actual billed + upcoming subscription revenue for any month. Better source for MRR than summing individual subscription prices.
- **Data**: Billed $, Upcoming $, Total $ — with daily breakdown chart
- **Filters**: Location, Month picker

### 3. Revenue by Location (MEDIUM PRIORITY)
- **URL**: TBD (under Sales)
- **Why**: Could split studio revenue by NOHO vs other locations

### 4. Unique Attendees (LOW PRIORITY)
- **URL**: TBD (under Customers)
- **Why**: Visit/attendance metrics beyond first visits

### 5. Retention (LOW PRIORITY)
- **URL**: TBD (under Auto-Renews)
- **Why**: Churn/retention analytics

---

## Revenue Category Names (from Revenue Categories report)

Categories seen in the report (alphabetical):
- 10MEMBER
- 10SKYTING
- 200HR Teacher Training
- A la carte SKY TING TV
- All Access Auto Renew Monthly
- COMMUNITY
- CUPPING
- Digital All Inclusive Monthly
- Donation Classes
- Drop-in (** key category for drop-in revenue **)
- Infrared Sauna
- Private
- SKY UNLIMITED (** largest subscription category **)
- SKY3
- SKY TING TV
- SKY TING TV 2025
- Spa Lounge
- Teacher Training
- Workshops
- ... and more

## Notes

- Revenue Categories report uses `/reports/revenue` (note: `reports` plural, unlike most other reports which use `/report/` singular)
- The "Cash" dropdown on Revenue Categories likely filters between cash and all payment types
- The date range defaults to last ~30 days (01/16/2026 - 02/16/2026)
- Recurring Revenue report uses monthly period selector, not date range
- The `/report/subscriptions/growth` base URL serves Net New, New, and Cancelled views via filter param
- The `/report/subscriptions/list` base URL serves Active and Paused views via status param
