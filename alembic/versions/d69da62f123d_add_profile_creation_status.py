"""add_profile_creation_status

Revision ID: d69da62f123d
Revises:
Create Date: 2025-04-10 22:10:22.219415

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd69da62f123d'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Добавляем новое поле profile_creation_status в таблицу users
    op.add_column(
        'users',
        sa.Column(
            'profile_creation_status',
            sa.String(length=20),
            nullable=True,
            server_default=None
        )
    )
    # Для существующих записей можно установить дефолтное значение
    # op.execute("UPDATE users SET profile_creation_status = 'pending' WHERE profile_creation_status IS NULL")


def downgrade() -> None:
    # Удаляем поле при откате миграции
    op.drop_column('users', 'profile_creation_status')