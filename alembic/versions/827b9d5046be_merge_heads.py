"""merge heads

Revision ID: 827b9d5046be
Revises: 1a40e144661a, f118841ab3b3
Create Date: 2025-04-18 21:16:13.538822

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '827b9d5046be'
down_revision: Union[str, None] = ('1a40e144661a', 'f118841ab3b3')
branch_labels: Union[str, Sequence[str], None] = ('main',)
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
