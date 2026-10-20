package com.example.payment.stat;

import com.example.payment.message.ShopTotalDTO;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import java.util.Optional;

/** 数据库唯一约束负责并发去重，条件Upsert负责防旧版本覆盖。 */
@Repository
public class ShopStatRepository {
    private static final String HANDLER = "shop-total-projection-v1";
    private final JdbcTemplate jdbc;
    public ShopStatRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }
    public boolean insertProcessed(ShopTotalDTO event) {
        return jdbc.update("""
                INSERT INTO processed_event(handler_name, event_id, shop_id, version) VALUES (?, ?, ?, ?)
                ON CONFLICT (handler_name, event_id) DO NOTHING
                """, HANDLER, event.resultId(), event.shopId(), event.version()) == 1;
    }
    public boolean upsertIfNewer(ShopTotalDTO event) {
        return jdbc.update("""
                INSERT INTO shop_payment_stat(shop_id, total_amount_cents, payment_count, version)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (shop_id) DO UPDATE SET
                    total_amount_cents = EXCLUDED.total_amount_cents,
                    payment_count = EXCLUDED.payment_count,
                    version = EXCLUDED.version,
                    updated_at = CURRENT_TIMESTAMP
                WHERE EXCLUDED.version > shop_payment_stat.version
                """, event.shopId(), event.totalAmountCents(), event.paymentCount(), event.version()) == 1;
    }
    public Optional<ShopStatDTO> find(String shopId) {
        return jdbc.query("""
                SELECT shop_id, total_amount_cents, payment_count, version
                FROM shop_payment_stat WHERE shop_id = ?
                """, (rs, row) -> new ShopStatDTO(rs.getString(1), rs.getLong(2),
                rs.getLong(3), rs.getLong(4)), shopId).stream().findFirst();
    }
    public long countProcessed() {
        return jdbc.queryForObject("SELECT count(*) FROM processed_event WHERE handler_name = ?", Long.class, HANDLER);
    }
}
