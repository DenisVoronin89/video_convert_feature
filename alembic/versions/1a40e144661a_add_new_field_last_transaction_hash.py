"""add new field last_transaction_hash

Revision ID: 1a40e144661a
Revises: None  # Убрана зависимость от предыдущей миграции
Create Date: 2025-04-15 20:05:20.739191

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa



# revision identifiers, used by Alembic.
revision: str = '1a40e144661a'
down_revision: Union[str, None] = None  # Теперь это самостоятельная миграция
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Добавляем новое поле в таблицу `users`
    op.add_column(
        'users',
        sa.Column(
            'last_transaction_hash',
            sa.String(length=150),
            nullable=True,
            unique=True,
            comment='Хэш последней транзакции пользователя'
        )
    )


def downgrade() -> None:
    # Удаляем поле при откате миграции
    op.drop_column('user_profile', 'last_transaction_hash')