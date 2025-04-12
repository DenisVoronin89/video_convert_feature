"""add new field profile_update_counter

Revision ID: f118841ab3b3
Revises: d69da62f123d
Create Date: 2025-04-12 15:18:22.443358
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f118841ab3b3'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'users',
        sa.Column('profile_update_counter', sa.Integer(), nullable=True, server_default="0"),
    )


def downgrade() -> None:
    op.drop_column('user_profile', 'profile_update_counter')
