"""add new fields is_adult_content and user_form_data

Revision ID: cc6c90afed6b
Revises: 827b9d5046be
Create Date: 2025-04-18 21:17:32.243979

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision: str = 'cc6c90afed6b'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Добавляем поле is_adult_content в user_profiles
    op.add_column('user_profiles',
                  sa.Column('is_adult_content',
                            sa.Boolean(),
                            server_default=sa.text('false'),
                            nullable=True))

    # Добавляем поле user_profile_form_data в users
    op.add_column('users',
                  sa.Column('user_profile_form_data',
                            JSONB(),
                            nullable=True))


def downgrade() -> None:
    # Удаляем поля при откате
    op.drop_column('user_profiles', 'is_adult_content')
    op.drop_column('users', 'user_profile_form_data')