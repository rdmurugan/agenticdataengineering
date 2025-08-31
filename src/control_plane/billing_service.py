"""
Billing and Usage-based Pricing Service for SaaS Platform
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from decimal import Decimal, ROUND_HALF_UP
import json
import logging
from pathlib import Path
from enum import Enum

logger = logging.getLogger(__name__)

class BillingPeriod(Enum):
    MONTHLY = "monthly"
    ANNUAL = "annual"

class InvoiceStatus(Enum):
    DRAFT = "draft"
    PENDING = "pending"
    PAID = "paid"
    OVERDUE = "overdue"
    CANCELLED = "cancelled"

@dataclass
class UsageMetric:
    """Individual usage metric for billing"""
    metric_name: str
    quantity: Decimal
    unit_price: Decimal
    total_cost: Decimal
    period_start: datetime
    period_end: datetime

@dataclass
class BillingPlan:
    """Billing plan configuration"""
    plan_id: str
    name: str
    base_monthly_fee: Decimal
    included_usage: Dict[str, int]  # Free tier limits
    overage_rates: Dict[str, Decimal]  # Per-unit pricing beyond free tier
    features: List[str]

@dataclass
class Invoice:
    """Customer invoice"""
    invoice_id: str
    tenant_id: str
    billing_period: BillingPeriod
    period_start: datetime
    period_end: datetime
    
    base_fee: Decimal
    usage_charges: List[UsageMetric]
    total_usage_cost: Decimal
    subtotal: Decimal
    tax_amount: Decimal
    total_amount: Decimal
    
    status: InvoiceStatus
    created_at: datetime
    due_date: datetime
    paid_at: Optional[datetime]

@dataclass
class BillingAccount:
    """Customer billing account"""
    tenant_id: str
    plan_id: str
    billing_period: BillingPeriod
    current_period_start: datetime
    current_period_end: datetime
    
    # Usage tracking
    current_usage: Dict[str, Decimal]
    usage_limits: Dict[str, int]
    
    # Billing info
    billing_contact: Dict[str, str]
    payment_method_id: Optional[str]
    auto_pay_enabled: bool
    
    # Account status
    account_status: str
    next_billing_date: datetime
    created_at: datetime

class BillingService:
    """Handles usage tracking, billing calculations, and invoice generation"""
    
    def __init__(self, storage_path: str = "data/billing"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        # Billing accounts
        self.billing_accounts: Dict[str, BillingAccount] = {}
        self.invoices: Dict[str, Invoice] = {}
        
        # Pricing plans
        self.billing_plans = self._initialize_billing_plans()
        
        # Load existing data
        self._load_billing_data()
        
        # Background billing task
        self._billing_task: Optional[asyncio.Task] = None
    
    def _initialize_billing_plans(self) -> Dict[str, BillingPlan]:
        """Initialize standard billing plans"""
        return {
            "starter": BillingPlan(
                plan_id="starter",
                name="Starter Plan",
                base_monthly_fee=Decimal("99.00"),
                included_usage={
                    "gb_processed": 100,
                    "compute_hours": 50,
                    "storage_gb": 500,
                    "quality_checks": 10000
                },
                overage_rates={
                    "gb_processed": Decimal("0.10"),
                    "compute_hours": Decimal("2.50"),
                    "storage_gb": Decimal("0.05"),
                    "quality_checks": Decimal("0.001")
                },
                features=["Basic quality monitoring", "Email alerts", "Standard support"]
            ),
            
            "professional": BillingPlan(
                plan_id="professional",
                name="Professional Plan", 
                base_monthly_fee=Decimal("299.00"),
                included_usage={
                    "gb_processed": 1000,
                    "compute_hours": 200,
                    "storage_gb": 2000,
                    "quality_checks": 50000
                },
                overage_rates={
                    "gb_processed": Decimal("0.08"),
                    "compute_hours": Decimal("2.00"),
                    "storage_gb": Decimal("0.04"),
                    "quality_checks": Decimal("0.0008")
                },
                features=["Advanced ML anomaly detection", "Custom quality rules", "Slack/PagerDuty alerts", "Priority support"]
            ),
            
            "enterprise": BillingPlan(
                plan_id="enterprise",
                name="Enterprise Plan",
                base_monthly_fee=Decimal("999.00"),
                included_usage={
                    "gb_processed": 10000,
                    "compute_hours": 1000,
                    "storage_gb": 10000,
                    "quality_checks": 250000
                },
                overage_rates={
                    "gb_processed": Decimal("0.06"),
                    "compute_hours": Decimal("1.50"),
                    "storage_gb": Decimal("0.03"),
                    "quality_checks": Decimal("0.0006")
                },
                features=["All professional features", "Dedicated support", "SLA guarantees", "Custom integrations"]
            ),
            
            "healthcare_plus": BillingPlan(
                plan_id="healthcare_plus",
                name="Healthcare Plus",
                base_monthly_fee=Decimal("1999.00"),
                included_usage={
                    "gb_processed": 50000,
                    "compute_hours": 5000,
                    "storage_gb": 50000,
                    "quality_checks": 1000000
                },
                overage_rates={
                    "gb_processed": Decimal("0.05"),
                    "compute_hours": Decimal("1.25"),
                    "storage_gb": Decimal("0.025"),
                    "quality_checks": Decimal("0.0005")
                },
                features=["All enterprise features", "HIPAA compliance", "PHI detection", "Dedicated compliance officer", "24/7 support"]
            )
        }
    
    def _load_billing_data(self):
        """Load existing billing accounts and invoices"""
        # Load billing accounts
        accounts_file = self.storage_path / "billing_accounts.json"
        if accounts_file.exists():
            try:
                with open(accounts_file, 'r') as f:
                    data = json.load(f)
                    for tenant_id, account_data in data.items():
                        # Convert datetime strings and decimals
                        account_data['current_period_start'] = datetime.fromisoformat(account_data['current_period_start'])
                        account_data['current_period_end'] = datetime.fromisoformat(account_data['current_period_end'])
                        account_data['next_billing_date'] = datetime.fromisoformat(account_data['next_billing_date'])
                        account_data['created_at'] = datetime.fromisoformat(account_data['created_at'])
                        account_data['billing_period'] = BillingPeriod(account_data['billing_period'])
                        
                        # Convert usage to Decimal
                        account_data['current_usage'] = {k: Decimal(str(v)) for k, v in account_data['current_usage'].items()}
                        
                        self.billing_accounts[tenant_id] = BillingAccount(**account_data)
                        
            except Exception as e:
                logger.error(f"Failed to load billing accounts: {e}")
        
        # Load invoices
        invoices_file = self.storage_path / "invoices.json"
        if invoices_file.exists():
            try:
                with open(invoices_file, 'r') as f:
                    data = json.load(f)
                    for invoice_id, invoice_data in data.items():
                        # Convert datetime strings and decimals
                        invoice_data['period_start'] = datetime.fromisoformat(invoice_data['period_start'])
                        invoice_data['period_end'] = datetime.fromisoformat(invoice_data['period_end'])
                        invoice_data['created_at'] = datetime.fromisoformat(invoice_data['created_at'])
                        invoice_data['due_date'] = datetime.fromisoformat(invoice_data['due_date'])
                        if invoice_data['paid_at']:
                            invoice_data['paid_at'] = datetime.fromisoformat(invoice_data['paid_at'])
                        
                        invoice_data['billing_period'] = BillingPeriod(invoice_data['billing_period'])
                        invoice_data['status'] = InvoiceStatus(invoice_data['status'])
                        
                        # Convert monetary amounts to Decimal
                        for field in ['base_fee', 'total_usage_cost', 'subtotal', 'tax_amount', 'total_amount']:
                            invoice_data[field] = Decimal(str(invoice_data[field]))
                        
                        # Convert usage metrics
                        usage_charges = []
                        for usage_data in invoice_data['usage_charges']:
                            usage_data['quantity'] = Decimal(str(usage_data['quantity']))
                            usage_data['unit_price'] = Decimal(str(usage_data['unit_price']))
                            usage_data['total_cost'] = Decimal(str(usage_data['total_cost']))
                            usage_data['period_start'] = datetime.fromisoformat(usage_data['period_start'])
                            usage_data['period_end'] = datetime.fromisoformat(usage_data['period_end'])
                            usage_charges.append(UsageMetric(**usage_data))
                        
                        invoice_data['usage_charges'] = usage_charges
                        self.invoices[invoice_id] = Invoice(**invoice_data)
                        
            except Exception as e:
                logger.error(f"Failed to load invoices: {e}")
    
    def _save_billing_data(self):
        """Persist billing data to storage"""
        # Save billing accounts
        accounts_data = {}
        for tenant_id, account in self.billing_accounts.items():
            account_dict = asdict(account)
            # Convert datetime objects and Decimal for JSON serialization
            account_dict['current_period_start'] = account.current_period_start.isoformat()
            account_dict['current_period_end'] = account.current_period_end.isoformat()
            account_dict['next_billing_date'] = account.next_billing_date.isoformat()
            account_dict['created_at'] = account.created_at.isoformat()
            account_dict['billing_period'] = account.billing_period.value
            account_dict['current_usage'] = {k: str(v) for k, v in account.current_usage.items()}
            accounts_data[tenant_id] = account_dict
        
        with open(self.storage_path / "billing_accounts.json", 'w') as f:
            json.dump(accounts_data, f, indent=2)
        
        # Save invoices
        invoices_data = {}
        for invoice_id, invoice in self.invoices.items():
            invoice_dict = asdict(invoice)
            # Convert datetime and Decimal objects
            invoice_dict['period_start'] = invoice.period_start.isoformat()
            invoice_dict['period_end'] = invoice.period_end.isoformat()
            invoice_dict['created_at'] = invoice.created_at.isoformat()
            invoice_dict['due_date'] = invoice.due_date.isoformat()
            if invoice.paid_at:
                invoice_dict['paid_at'] = invoice.paid_at.isoformat()
            
            invoice_dict['billing_period'] = invoice.billing_period.value
            invoice_dict['status'] = invoice.status.value
            
            # Convert monetary amounts
            for field in ['base_fee', 'total_usage_cost', 'subtotal', 'tax_amount', 'total_amount']:
                invoice_dict[field] = str(getattr(invoice, field))
            
            # Convert usage charges
            usage_charges_data = []
            for usage in invoice.usage_charges:
                usage_dict = asdict(usage)
                usage_dict['quantity'] = str(usage.quantity)
                usage_dict['unit_price'] = str(usage.unit_price)
                usage_dict['total_cost'] = str(usage.total_cost)
                usage_dict['period_start'] = usage.period_start.isoformat()
                usage_dict['period_end'] = usage.period_end.isoformat()
                usage_charges_data.append(usage_dict)
            
            invoice_dict['usage_charges'] = usage_charges_data
            invoices_data[invoice_id] = invoice_dict
        
        with open(self.storage_path / "invoices.json", 'w') as f:
            json.dump(invoices_data, f, indent=2)
    
    async def initialize_tenant_billing(self, tenant_id: str, plan_id: str = "starter", billing_period: BillingPeriod = BillingPeriod.MONTHLY):
        """Initialize billing account for new tenant"""
        if tenant_id in self.billing_accounts:
            logger.warning(f"Billing account already exists for tenant {tenant_id}")
            return
        
        plan = self.billing_plans.get(plan_id)
        if not plan:
            raise ValueError(f"Invalid billing plan: {plan_id}")
        
        now = datetime.utcnow()
        if billing_period == BillingPeriod.MONTHLY:
            period_end = now.replace(day=1) + timedelta(days=32)
            period_end = period_end.replace(day=1) - timedelta(days=1)
        else:  # Annual
            period_end = now.replace(year=now.year + 1, month=1, day=1) - timedelta(days=1)
        
        billing_account = BillingAccount(
            tenant_id=tenant_id,
            plan_id=plan_id,
            billing_period=billing_period,
            current_period_start=now.replace(day=1),
            current_period_end=period_end,
            
            current_usage={
                "gb_processed": Decimal("0"),
                "compute_hours": Decimal("0"),
                "storage_gb": Decimal("0"),
                "quality_checks": Decimal("0")
            },
            usage_limits=plan.included_usage,
            
            billing_contact={},
            payment_method_id=None,
            auto_pay_enabled=False,
            
            account_status="active",
            next_billing_date=period_end + timedelta(days=1),
            created_at=now
        )
        
        self.billing_accounts[tenant_id] = billing_account
        self._save_billing_data()
        
        logger.info(f"Initialized billing for tenant {tenant_id} with plan {plan_id}")
    
    async def track_usage(self, tenant_id: str, metric: str, quantity: Decimal):
        """Track usage for billing calculations"""
        if tenant_id not in self.billing_accounts:
            logger.warning(f"No billing account for tenant {tenant_id}")
            return
        
        account = self.billing_accounts[tenant_id]
        
        if metric in account.current_usage:
            account.current_usage[metric] += quantity
        else:
            account.current_usage[metric] = quantity
        
        self._save_billing_data()
        logger.debug(f"Tracked {quantity} {metric} for tenant {tenant_id}")
    
    async def generate_invoice(self, tenant_id: str) -> Invoice:
        """Generate invoice for tenant's current billing period"""
        if tenant_id not in self.billing_accounts:
            raise ValueError(f"No billing account for tenant {tenant_id}")
        
        account = self.billing_accounts[tenant_id]
        plan = self.billing_plans[account.plan_id]
        
        # Calculate usage charges
        usage_charges = []
        total_usage_cost = Decimal("0")
        
        for metric, used_quantity in account.current_usage.items():
            included_quantity = Decimal(str(account.usage_limits.get(metric, 0)))
            overage_quantity = max(Decimal("0"), used_quantity - included_quantity)
            
            if overage_quantity > 0:
                unit_price = plan.overage_rates.get(metric, Decimal("0"))
                total_cost = (overage_quantity * unit_price).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                
                if total_cost > 0:
                    usage_charge = UsageMetric(
                        metric_name=metric,
                        quantity=overage_quantity,
                        unit_price=unit_price,
                        total_cost=total_cost,
                        period_start=account.current_period_start,
                        period_end=account.current_period_end
                    )
                    usage_charges.append(usage_charge)
                    total_usage_cost += total_cost
        
        # Calculate totals
        base_fee = plan.base_monthly_fee
        if account.billing_period == BillingPeriod.ANNUAL:
            base_fee = base_fee * 12 * Decimal("0.9")  # 10% annual discount
        
        subtotal = base_fee + total_usage_cost
        tax_rate = Decimal("0.08")  # 8% tax rate
        tax_amount = (subtotal * tax_rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        total_amount = subtotal + tax_amount
        
        # Create invoice
        invoice_id = f"inv_{tenant_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        invoice = Invoice(
            invoice_id=invoice_id,
            tenant_id=tenant_id,
            billing_period=account.billing_period,
            period_start=account.current_period_start,
            period_end=account.current_period_end,
            
            base_fee=base_fee,
            usage_charges=usage_charges,
            total_usage_cost=total_usage_cost,
            subtotal=subtotal,
            tax_amount=tax_amount,
            total_amount=total_amount,
            
            status=InvoiceStatus.PENDING,
            created_at=datetime.utcnow(),
            due_date=datetime.utcnow() + timedelta(days=30),
            paid_at=None
        )
        
        self.invoices[invoice_id] = invoice
        self._save_billing_data()
        
        logger.info(f"Generated invoice {invoice_id} for tenant {tenant_id}: ${total_amount}")
        return invoice
    
    async def start_billing_cycle(self):
        """Start the recurring billing cycle background task"""
        if self._billing_task and not self._billing_task.done():
            logger.warning("Billing cycle already running")
            return
        
        self._billing_task = asyncio.create_task(self._billing_cycle_loop())
        logger.info("Started billing cycle")
    
    async def _billing_cycle_loop(self):
        """Main billing cycle loop"""
        while True:
            try:
                await self._process_billing_cycle()
                # Check every hour
                await asyncio.sleep(3600)
                
            except Exception as e:
                logger.error(f"Error in billing cycle: {e}")
                await asyncio.sleep(3600)
    
    async def _process_billing_cycle(self):
        """Process billing for all accounts that are due"""
        now = datetime.utcnow()
        
        for tenant_id, account in self.billing_accounts.items():
            if now >= account.next_billing_date:
                try:
                    # Generate invoice for completed period
                    invoice = await self.generate_invoice(tenant_id)
                    
                    # Reset usage tracking for new period
                    await self._start_new_billing_period(tenant_id)
                    
                    logger.info(f"Processed billing for tenant {tenant_id}")
                    
                except Exception as e:
                    logger.error(f"Failed to process billing for tenant {tenant_id}: {e}")
    
    async def _start_new_billing_period(self, tenant_id: str):
        """Start new billing period for tenant"""
        account = self.billing_accounts[tenant_id]
        
        # Reset usage counters
        account.current_usage = {metric: Decimal("0") for metric in account.current_usage}
        
        # Update billing period
        account.current_period_start = account.current_period_end + timedelta(days=1)
        
        if account.billing_period == BillingPeriod.MONTHLY:
            next_month = account.current_period_start.replace(day=1) + timedelta(days=32)
            account.current_period_end = next_month.replace(day=1) - timedelta(days=1)
        else:  # Annual
            account.current_period_end = account.current_period_start.replace(year=account.current_period_start.year + 1) - timedelta(days=1)
        
        account.next_billing_date = account.current_period_end + timedelta(days=1)
        
        self._save_billing_data()
    
    async def get_tenant_billing(self, tenant_id: str) -> Dict[str, Any]:
        """Get comprehensive billing information for tenant"""
        if tenant_id not in self.billing_accounts:
            raise ValueError(f"No billing account for tenant {tenant_id}")
        
        account = self.billing_accounts[tenant_id]
        plan = self.billing_plans[account.plan_id]
        
        # Get recent invoices
        tenant_invoices = [
            {
                "invoice_id": inv.invoice_id,
                "period_start": inv.period_start.isoformat(),
                "period_end": inv.period_end.isoformat(),
                "total_amount": str(inv.total_amount),
                "status": inv.status.value,
                "created_at": inv.created_at.isoformat()
            }
            for inv in self.invoices.values()
            if inv.tenant_id == tenant_id
        ]
        
        # Calculate usage percentages
        usage_percentages = {}
        for metric, used in account.current_usage.items():
            limit = account.usage_limits.get(metric, 0)
            if limit > 0:
                usage_percentages[metric] = float((used / Decimal(str(limit))) * 100)
            else:
                usage_percentages[metric] = 0.0
        
        return {
            "account": {
                "tenant_id": tenant_id,
                "plan_id": account.plan_id,
                "plan_name": plan.name,
                "billing_period": account.billing_period.value,
                "account_status": account.account_status,
                "current_period_start": account.current_period_start.isoformat(),
                "current_period_end": account.current_period_end.isoformat(),
                "next_billing_date": account.next_billing_date.isoformat()
            },
            "current_usage": {k: str(v) for k, v in account.current_usage.items()},
            "usage_limits": account.usage_limits,
            "usage_percentages": usage_percentages,
            "plan_features": plan.features,
            "recent_invoices": tenant_invoices[-5:],  # Last 5 invoices
            "base_monthly_fee": str(plan.base_monthly_fee)
        }
    
    async def update_billing_plan(self, tenant_id: str, new_plan_id: str):
        """Update tenant's billing plan"""
        if tenant_id not in self.billing_accounts:
            raise ValueError(f"No billing account for tenant {tenant_id}")
        
        if new_plan_id not in self.billing_plans:
            raise ValueError(f"Invalid billing plan: {new_plan_id}")
        
        account = self.billing_accounts[tenant_id]
        new_plan = self.billing_plans[new_plan_id]
        
        # Update account
        account.plan_id = new_plan_id
        account.usage_limits = new_plan.included_usage.copy()
        
        self._save_billing_data()
        
        logger.info(f"Updated billing plan for tenant {tenant_id} to {new_plan_id}")
    
    def get_invoice(self, invoice_id: str) -> Optional[Invoice]:
        """Get invoice by ID"""
        return self.invoices.get(invoice_id)
    
    async def mark_invoice_paid(self, invoice_id: str):
        """Mark invoice as paid"""
        if invoice_id not in self.invoices:
            raise ValueError(f"Invoice {invoice_id} not found")
        
        invoice = self.invoices[invoice_id]
        invoice.status = InvoiceStatus.PAID
        invoice.paid_at = datetime.utcnow()
        
        self._save_billing_data()
        
        logger.info(f"Marked invoice {invoice_id} as paid")