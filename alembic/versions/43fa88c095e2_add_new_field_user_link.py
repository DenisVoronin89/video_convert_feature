"""add user_link field to user_profiles

Revision ID: 43fa88c095e2
Create Date: 2025-03-29 11:06:28.699433

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '43fa88c095e2'
down_revision = None  # УБРАНО НАХУЙ
branch_labels = None
depends_on = None  # УБРАНО НАХУЙ


def upgrade():
    # ЧИСТО ДОБАВЛЯЕМ user_link БЕЗ ЛИШНИХ ПРОВЕРОК
    op.add_column(
        'user_profiles',
        sa.Column('user_link', sa.String(length=255), nullable=True)
    )
    op.create_unique_constraint(
        'uq_user_profiles_user_link',
        'user_profiles',
        ['user_link']
    )


def downgrade():
    # ЧИСТО ОТКАТ БЕЗ ЛИШНЕГО ГОВНА
    op.drop_constraint(
        'uq_user_profiles_user_link',
        'user_profiles',
        type_='unique'
    )
    op.drop_column('user_profiles', 'user_link')